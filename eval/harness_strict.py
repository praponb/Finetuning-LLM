"""Type-strict re-check of model replies on eval/harness_suite.jsonl, with the harness's own comparison.

eval/evaluate.py (verify_pairs.compare) checks column names + rows, with every number rounded to a float, so it
cannot see DECIMAL vs DOUBLE, INT vs BIGINT and similar type slips. This script takes the replies stored by
evaluate.py and runs each one through sql2pyspark_harness/compare_harness.compare_one: same column names AND Spark
data types, same multiset of rows (doubles to 6 places), every value forced to compute. No model is queried.

  python eval/harness_strict.py eval/results/harness/v18a-q4__harness_suite.json [more result files]
  python eval/harness_strict.py --v2 eval/results/harness/v18a-q4__harness_suite_v2.json   (50-query set, 13 tables)
    -> prints PASS / FAIL_SCHEMA / FAIL_DATA / ERROR_PYSPARK per case, writes <file>.strict.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import evaluate as ev           # noqa: E402  (extract_code)
import harness_tables as ht     # noqa: E402  (load_suite: the harness parser)



def builder(code: str, tables: list[str]):
    def build(spark):
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        ns = {"F": F, "Window": Window, "spark": spark} | {f"df_{t}": spark.table(t) for t in tables}
        exec(compile(code, "<pyspark>", "exec"), ns)
        return ns.get("result")
    return build


def main(paths: list[str]) -> None:
    if paths and paths[0] == "--v2":        # eval/harness_suite_v2.jsonl: sql2pyspark_sql_suite/suite_all.sql
        import harness_tables_v2 as h2
        paths, stmts = paths[1:], h2.load_suite()
    else:
        stmts = ht.load_suite()
    ch = sys.modules["compare_harness"]       # loaded by load_suite()
    by_id = {f"harness_{s.id}": s for s in stmts if not s.is_setup}
    tables = [s.id.split(":", 1)[1] for s in stmts if s.is_setup]
    spark = ch.make_spark()
    ch.run_setup(spark, stmts)
    for p in paths:
        data = json.loads(Path(p).read_text())
        out, counts = [], {}
        for c in data["cases"]:
            code = ev.extract_code(c["reply"])
            r = ch.compare_one(spark, by_id[c["id"]], builder(code, tables), ch.Options())
            counts[r.status] = counts.get(r.status, 0) + 1
            out.append({"id": c["id"], "status": r.status, "message": r.message[:400],
                        "loose_match": c["strict"]["match"]})
        n_pass = counts.get("PASS", 0)
        print(f"{data['model']}: strict-type PASS {n_pass}/{len(out)}  "
              f"(our checker: {sum(o['loose_match'] for o in out)}/{len(out)})  {counts}")
        for o in out:
            print(f"   {o['id'][8:]:5} {o['status']:14} {o['message'].splitlines()[0][:110] if o['message'] else ''}")
        Path(p).with_suffix(".strict.json").write_text(json.dumps({"model": data["model"], "counts": counts,
                                                                   "cases": out}, indent=2))


if __name__ == "__main__":
    main(sys.argv[1:])
