"""Execution check for the complex_v8 pairs (same rules as ../verify_pairs.py, different tables).

Both sides run in local Spark against the complex_v8 catalog: the SQL with spark.sql over temp views, the
PySpark code exec'd with df_<table> DataFrames bound (it must assign `result`). Column names, rows (as a
multiset, floats to 4 decimals) and the ORDER BY columns' sequence must match. The comparison itself is
imported from ../verify_pairs.py, unchanged.

Usage:  python verify.py --src raw_pairs --workers 5
        -> raw_pairs/verified.jsonl, verify_report.json, verify_failures.jsonl
"""
from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import os
import sys
import time
import warnings
from collections import Counter, defaultdict
from dataclasses import asdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(1, str(HERE.parent))
warnings.filterwarnings("ignore")

from verify_pairs import Result, _short, compare, load_pairs, rows_of  # noqa: E402  (existing code, reused)


class CxChecker:
    def __init__(self):
        from pyspark.sql import DataFrame, SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        from catalog import make_dataframes

        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        self.spark = (SparkSession.builder.master("local[1]").appName("verify-complex-v8")
                      .config("spark.ui.enabled", "false")
                      .config("spark.sql.shuffle.partitions", "1")
                      .config("spark.default.parallelism", "1")
                      .config("spark.driver.memory", "1g")
                      .config("spark.sql.session.timeZone", "UTC")
                      .config("spark.sql.adaptive.enabled", "false")
                      .config("spark.ui.showConsoleProgress", "false")
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel("OFF")
        for name in ("SQLQueryContextLogger", "py4j", "pyspark"):
            logging.getLogger(name).setLevel(logging.CRITICAL + 1)
        self.frames = {k: v.cache() for k, v in make_dataframes(self.spark).items()}
        for df in self.frames.values():
            df.count()
        self.base_ns = {"F": F, "Window": Window, "spark": self.spark}
        self.DataFrame = DataFrame

    def check(self, sql: str, code: str, order_cols=()) -> Result:
        try:
            df = self.spark.sql(sql.strip().rstrip(";"))
            s_cols, s_rows = df.columns, rows_of(df)
        except Exception as e:  # noqa: BLE001
            return Result(False, "sql_error", _short(e))
        try:
            ns = dict(self.base_ns)
            ns.update(self.frames)
            exec(compile(code, "<pyspark>", "exec"), ns)
            res = ns.get("result")
            if not isinstance(res, self.DataFrame):
                return Result(False, "no_result", "code did not assign a DataFrame to `result`")
            p_cols, p_rows = res.columns, rows_of(res)
        except Exception as e:  # noqa: BLE001
            return Result(False, "py_error", _short(e))
        return compare(s_cols, s_rows, p_cols, p_rows, order_cols)


_checker: CxChecker | None = None


def _init_worker():
    global _checker
    _checker = CxChecker()


def _work(rec):
    assert _checker is not None
    r = _checker.check(rec["sql"], rec["code"], rec.get("order_cols") or ())
    return rec, asdict(r)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", default="raw_pairs")
    ap.add_argument("--workers", type=int, default=5)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    src = Path(args.src)
    recs = load_pairs(src)
    if args.limit:
        recs = recs[: args.limit]
    print(f"checking {len(recs)} pairs with {args.workers} workers ...", flush=True)
    t0 = time.time()
    ok, fails = [], []
    by_status, by_family = Counter(), defaultdict(Counter)
    with mp.get_context("spawn").Pool(args.workers, initializer=_init_worker) as pool:
        for i, (rec, res) in enumerate(pool.imap_unordered(_work, recs, chunksize=8), 1):
            by_status[res["status"]] += 1
            by_family[rec.get("family", "unknown")][res["status"]] += 1
            (ok if res["ok"] else fails).append({**rec, "n_rows": res["n_rows"]} if res["ok"] else {**rec, **res})
            if i % 500 == 0:
                print(f"  {i}/{len(recs)}  ok={len(ok)}  ({time.time() - t0:.0f}s)", flush=True)
    ok.sort(key=lambda r: r["id"])
    with open(src / "verified.jsonl", "w", encoding="utf-8") as f:
        for r in ok:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(src / "verify_failures.jsonl", "w", encoding="utf-8") as f:
        for r in sorted(fails, key=lambda r: r["id"]):
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    report = {
        "total": len(recs), "ok": len(ok), "pass_rate": round(len(ok) / max(1, len(recs)), 4),
        "ok_but_empty_result": sum(1 for r in ok if r["n_rows"] == 0), "by_status": dict(by_status),
        "by_family": {k: dict(v) for k, v in sorted(by_family.items())}, "seconds": round(time.time() - t0, 1),
    }
    (src / "verify_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
