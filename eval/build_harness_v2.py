"""Build eval/harness_suite_v2.jsonl: the queries of sql2pyspark_sql_suite/suite_all.sql as eval cases (51 minus SKIP = 50).

ids harness_<QID> (the 21 Part-1 ids are the same as in eval/harness_suite.jsonl), level = part1 / part2 from the
suite's manifest.json, so evaluate.py's summary scores the old 21 and the new 29 separately. TEST ONLY: never put
these queries (or their tables) into training data.

  python eval/build_harness_v2.py
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import harness_tables_v2 as h2

ROOT = Path(__file__).resolve().parent.parent
# left out of the eval set (the queries stay in the suite):
SKIP = {
    # corr(quantity, unit_usd) over a group where one column is constant: with one partition (verify_pairs runs
    # Spark local[1]) the variance is exactly 0 and ANSI mode raises DIVIDE_BY_ZERO; local[*] gets a tiny non-zero
    # float and passes. So the SQL result depends on partitioning, and a correct F.corr answer would fail too.
    "Q20a": "corr over a zero-variance group: DIVIDE_BY_ZERO under local[1] (partition-dependent)",
}
manifest = json.loads((ROOT / "sql2pyspark_sql_suite" / "manifest.json").read_text())
part = {s["id"]: s["part"] for s in manifest["statements"]}
title = {s["id"]: s.get("title", "") for s in manifest["statements"]}
rows = []
for s in h2.load_suite():
    if s.is_setup or s.id in SKIP:
        continue
    rows.append({"id": f"harness_{s.id}", "level": part.get(s.id, "part2"), "family": re.sub(r"[a-z]$", "", s.id),
                 "title": title.get(s.id, ""), "sql": s.sql, "order_cols": []})
out = ROOT / "eval" / "harness_suite_v2.jsonl"
out.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows))
print(f"{len(rows)} cases -> {out}  ({sum(r['level'] == 'part1' for r in rows)} part1, "
      f"{sum(r['level'] == 'part2' for r in rows)} part2; skipped {sorted(SKIP)})")
