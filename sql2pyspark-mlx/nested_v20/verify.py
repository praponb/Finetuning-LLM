"""Verify nested_v20 candidates in Spark: keep a pair only if SQL and PySpark agree TYPE-STRICT and in ORDER BY order.

Per candidate: register its two tables (typed SELECT ... FROM VALUES), run the SQL (2-60 rows), run the code, then
sql2pyspark_harness/compare_harness.compare_one (column names + Spark types + row multiset, every value computed) and
verify_pairs.compare with order_cols. Worker processes each own a local Spark session.

  .venv/bin/python nested_v20/verify.py nested_v20/raw/cands.jsonl nested_v20/raw/verified.jsonl --workers 4
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from multiprocessing import Pool
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
REPO = MLX.parent
_S = None


def _init():
    global _S
    sys.path.insert(0, str(MLX))
    sys.path.insert(0, str(REPO))
    sys.path.insert(0, str(REPO / "sql2pyspark_harness"))
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession
    _S = (SparkSession.builder.master("local[2]").appName("nested-v20-verify")
          .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2")
          .config("spark.driver.memory", "2g").config("spark.sql.session.timeZone", "UTC")
          .config("spark.ui.showConsoleProgress", "false").getOrCreate())
    _S.sparkContext.setLogLevel("OFF")
    import logging
    for name in ("py4j", "pyspark", "SQLQueryContextLogger", "DataFrameQueryContextLogger"):
        logging.getLogger(name).setLevel(logging.CRITICAL + 1)


def check(rec: dict) -> dict:
    import compare_harness as ch
    import kimi_repair as kr
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from verify_pairs import compare, rows_of
    spark = _S.newSession()
    ns = {"F": F, "Window": Window, "spark": spark}
    try:
        for name, tsql in rec["tables"].items():
            df = spark.sql(tsql)
            df.createOrReplaceTempView(name)
            ns[f"df_{name}"] = df
        sdf = spark.sql(rec["sql"])
        s_cols, s_rows = sdf.columns, rows_of(sdf)
    except Exception as e:  # noqa: BLE001
        return {**rec, "status": "sql_error", "msg": kr.short_error(e)[:300]}
    if not 2 <= len(s_rows) <= 60:
        return {**rec, "status": "rows", "msg": f"{len(s_rows)} rows"}
    if any(all(r[i] is None for r in s_rows) for i in range(len(s_cols))):
        return {**rec, "status": "null_col", "msg": "a result column is NULL in every row"}
    try:
        kr.exec_code(rec["code"], dict(ns))
    except Exception as e:  # noqa: BLE001
        return {**rec, "status": "py_error", "msg": kr.short_error(e)[:300]}
    r = ch.compare_one(spark, ch.Statement("case", rec["sql"]), lambda _s: kr.exec_code(rec["code"], dict(ns)),
                       ch.Options())
    if r.status != ch.PASS:
        return {**rec, "status": r.status, "msg": r.message[:300]}
    res = kr.exec_code(rec["code"], dict(ns))
    lr = compare(s_cols, s_rows, res.columns, rows_of(res), rec["order_cols"])
    if not lr.ok:
        return {**rec, "status": "order", "msg": lr.detail[:300]}
    return {**rec, "status": "ok", "rows": len(s_rows)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("inp")
    ap.add_argument("out")
    ap.add_argument("--workers", type=int, default=4)
    a = ap.parse_args()
    recs = [json.loads(l) for l in open(a.inp)]
    stats, fam = Counter(), Counter()
    with Pool(a.workers, initializer=_init) as pool, open(a.out, "w") as f:
        for i, r in enumerate(pool.imap(check, recs, chunksize=8), 1):
            stats[r["status"]] += 1
            fam[(r["family"], r["status"] == "ok")] += 1
            f.write(json.dumps(r) + "\n")
            if i % 500 == 0:
                print(f"  {i}/{len(recs)} {dict(stats)}", flush=True)
    print("status:", dict(stats))
    for name in sorted({k for k, _ in fam}):
        ok, bad = fam[(name, True)], fam[(name, False)]
        print(f"  {name:16s} ok {ok:5d} / {ok + bad:5d}")


if __name__ == "__main__":
    main()
