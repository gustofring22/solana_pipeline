#!/usr/bin/env python3
"""Index every transaction that touches a Solana program into a local DuckDB file.

    python index_program.py --program <PROGRAM_ID> --limit 2000

Reads SOLANA_RPC_URL from the environment or from a .env file in the current folder.
Re-running is safe: transactions already stored are skipped, so it resumes where it stopped.
"""
import argparse
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import base58
import duckdb
import httpx

# Phoenix, used only as a default so you can test the script. Pass --program for your own.
DEFAULT_PROGRAM = "EtrnLzgbS7nMMy5fbD42kXiUzGg8XQzJ972Xtk1cjWih"

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    signature     VARCHAR PRIMARY KEY,
    slot          BIGINT,
    block_time    TIMESTAMP,
    success       BOOLEAN,
    error         VARCHAR,
    fee_lamports  BIGINT,
    compute_units BIGINT,
    fee_payer     VARCHAR,
    version       VARCHAR
);
CREATE TABLE IF NOT EXISTS instructions (
    signature     VARCHAR,
    ix_index      INTEGER,   -- position of the top-level instruction
    inner_index   INTEGER,   -- -1 = top-level, 0.. = CPI call made inside it
    program_id    VARCHAR,
    accounts      VARCHAR[],
    data_hex      VARCHAR,
    discriminator VARCHAR,   -- first 8 bytes as hex (the Anchor instruction/event id)
    PRIMARY KEY (signature, ix_index, inner_index)
);
CREATE TABLE IF NOT EXISTS skipped_signatures (
    signature VARCHAR PRIMARY KEY  -- the RPC could not return these; not retried
);
CREATE TABLE IF NOT EXISTS program_data_logs (
    signature   VARCHAR,
    log_index   INTEGER,
    payload_b64 VARCHAR,     -- Anchor emit!() events, still encoded
    PRIMARY KEY (signature, log_index)
);
"""


def load_env_file(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


class RPC:
    """Tiny JSON-RPC client with a shared rate limit and retry on 429/5xx."""

    def __init__(self, url, requests_per_second):
        self.url = url
        self.rate = requests_per_second
        self.client = httpx.Client(timeout=60)
        self.lock = threading.Lock()
        self.next_free = 0.0

    def _throttle(self, weight):
        with self.lock:
            now = time.monotonic()
            start = max(now, self.next_free)
            self.next_free = start + weight / self.rate
        if start > now:
            time.sleep(start - now)

    def _post(self, payload, weight):
        self._throttle(weight)
        for attempt in range(8):
            try:
                r = self.client.post(self.url, json=payload)
            except httpx.TransportError as e:
                wait = min(2 ** attempt, 30)
                print(f"  network error ({e}); retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = min(2 ** attempt, 30)
                print(f"  HTTP {r.status_code}; retrying in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if r.status_code >= 400:
                sys.exit(f"RPC rejected the request (HTTP {r.status_code}). Check SOLANA_RPC_URL.")
            return r.json()
        sys.exit("RPC kept failing after 8 retries. Lower --rps or try again later.")

    def call(self, method, params):
        body = self._post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, 1)
        if "error" in body:
            sys.exit(f"RPC error from {method}: {body['error']}")
        return body["result"]

    def get_transactions(self, signatures):
        """One batched request. Returns {signature: result-or-None}."""
        payload = [
            {"jsonrpc": "2.0", "id": i, "method": "getTransaction",
             "params": [s, {"encoding": "json", "maxSupportedTransactionVersion": 0,
                            "commitment": "finalized"}]}
            for i, s in enumerate(signatures)
        ]
        body = self._post(payload, len(payload))
        if isinstance(body, dict):  # whole batch rejected
            sys.exit(f"RPC rejected the batch: {body.get('error', body)}")
        out = {}
        for item in body:
            sig = signatures[item["id"]]
            out[sig] = None if "error" in item else item.get("result")
        return out


def signature_pages(rpc, program, before=None):
    """Yield pages of signatures, newest first, walking backwards in time."""
    while True:
        opts = {"limit": 1000, "commitment": "finalized"}
        if before:
            opts["before"] = before
        page = rpc.call("getSignaturesForAddress", [program, opts])
        if not page:
            return
        yield page
        before = page[-1]["signature"]
        if len(page) < 1000:
            return


def parse_transaction(signature, tx, program):
    """Turn one raw getTransaction result into rows for our three tables."""
    message = tx["transaction"]["message"]
    meta = tx.get("meta") or {}

    keys = list(message["accountKeys"])
    loaded = meta.get("loadedAddresses") or {}  # accounts pulled in via lookup tables
    keys += loaded.get("writable", []) + loaded.get("readonly", [])

    block_time = tx.get("blockTime")
    block_time = (datetime.fromtimestamp(block_time, tz=timezone.utc).replace(tzinfo=None)
                  if block_time else None)
    err = meta.get("err")
    tx_row = (
        signature, tx.get("slot"), block_time, err is None,
        None if err is None else str(err),
        meta.get("fee"), meta.get("computeUnitsConsumed"),
        keys[0] if keys else None, str(tx.get("version", "legacy")),
    )

    ix_rows = []

    def add(ix_index, inner_index, ix):
        program_id = keys[ix["programIdIndex"]]
        if program_id != program:
            return
        raw = base58.b58decode(ix["data"]) if ix.get("data") else b""
        ix_rows.append((
            signature, ix_index, inner_index, program_id,
            [keys[i] for i in ix.get("accounts", [])],
            raw.hex(), raw[:8].hex(),
        ))

    for i, ix in enumerate(message["instructions"]):
        add(i, -1, ix)
    for group in meta.get("innerInstructions") or []:
        for j, ix in enumerate(group["instructions"]):
            add(group["index"], j, ix)

    log_rows = []
    for n, line in enumerate(meta.get("logMessages") or []):
        if line.startswith("Program data: "):
            log_rows.append((signature, n, line[len("Program data: "):]))

    return tx_row, ix_rows, log_rows


def save(con, tx_rows, ix_rows, log_rows):
    if tx_rows:
        con.executemany("INSERT OR IGNORE INTO transactions VALUES (?,?,?,?,?,?,?,?,?)", tx_rows)
    if ix_rows:
        con.executemany("INSERT OR IGNORE INTO instructions VALUES (?,?,?,?,?,?,?)", ix_rows)
    if log_rows:
        con.executemany("INSERT OR IGNORE INTO program_data_logs VALUES (?,?,?)", log_rows)


def known_signatures(con, signatures):
    rows = con.execute(
        "SELECT signature FROM transactions WHERE list_contains(?, signature) "
        "UNION SELECT signature FROM skipped_signatures WHERE list_contains(?, signature)",
        [signatures, signatures],
    ).fetchall()
    return {r[0] for r in rows}


def run_once(con, rpc, args):
    before = None
    if args.backfill:
        row = con.execute("SELECT signature FROM transactions ORDER BY slot ASC LIMIT 1").fetchone()
        before = row[0] if row else None
        if before:
            print(f"Backfilling older than slot of {before[:12]}...")

    stored = 0
    skipped_missing = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for page in signature_pages(rpc, args.program, before):
            sigs = [p["signature"] for p in page]
            known = set() if args.backfill else known_signatures(con, sigs)
            todo = [s for s in sigs if s not in known]

            # Catch-up mode: a whole page we already have means we have caught up.
            if not todo and not args.backfill:
                print("Caught up with what is already stored.")
                return stored

            if args.limit:
                todo = todo[: max(0, args.limit - stored)]

            chunks = [todo[i:i + args.batch] for i in range(0, len(todo), args.batch)]
            for result in pool.map(rpc.get_transactions, chunks):  # keeps newest-first order
                tx_rows, ix_rows, log_rows = [], [], []
                for sig, tx in result.items():
                    if tx is None:
                        skipped_missing += 1
                        con.execute("INSERT OR IGNORE INTO skipped_signatures VALUES (?)", [sig])
                        continue
                    t, i, l = parse_transaction(sig, tx, args.program)
                    tx_rows.append(t)
                    ix_rows += i
                    log_rows += l
                save(con, tx_rows, ix_rows, log_rows)
                stored += len(tx_rows)
                print(f"  stored {stored} transactions", end="\r", flush=True)

            if args.limit and stored >= args.limit:
                print(f"\nReached --limit {args.limit}.")
                return stored
    print()
    if skipped_missing:
        print(f"{skipped_missing} transactions were not available from the RPC and were skipped.")
    return stored


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--program", default=DEFAULT_PROGRAM, help="program ID to index")
    parser.add_argument("--db", default="solana.duckdb", help="DuckDB file to write")
    parser.add_argument("--limit", type=int, default=2000,
                        help="max new transactions per run (0 = no limit)")
    parser.add_argument("--backfill", action="store_true",
                        help="continue into older history instead of catching up to the newest")
    parser.add_argument("--follow", type=int, default=0, metavar="SECONDS",
                        help="keep running, checking for new transactions every N seconds")
    parser.add_argument("--rps", type=float, default=20,
                        help="max transactions fetched per second (match your RPC plan)")
    parser.add_argument("--batch", type=int, default=20, help="transactions per batched request")
    parser.add_argument("--workers", type=int, default=4, help="parallel requests")
    args = parser.parse_args()

    load_env_file()
    url = os.environ.get("SOLANA_RPC_URL")
    if not url:
        sys.exit("SOLANA_RPC_URL is not set. Put it in a .env file or your environment.")

    con = duckdb.connect(args.db)
    con.execute(SCHEMA)
    rpc = RPC(url, args.rps)

    print(f"Indexing program {args.program} into {args.db}")
    while True:
        run_once(con, rpc, args)
        total = con.execute("SELECT count(*) FROM transactions").fetchone()[0]
        print(f"Database now holds {total} transactions.")
        if not args.follow or args.backfill:
            break
        time.sleep(args.follow)


if __name__ == "__main__":
    main()