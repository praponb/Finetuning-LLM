"""Score an Ollama model on the Kimi-built real-style pairs (realstyle_v19/pilot/cases.jsonl): long (80-150 line)
SQL over each case's OWN tables (SELECT * FROM VALUES text), so eval/evaluate.py's single global --tables set does not
fit. Same prompt, code extraction, strict / lenient scoring and results JSON layout as eval/evaluate.py.

Usage (from the repo root):
  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/realstyle_v19/eval_realstyle.py --model sql2pyspark:q35moe-v1-q4 --think false
  sql2pyspark-mlx/.venv/bin/python sql2pyspark-mlx/realstyle_v19/eval_realstyle.py --model sql2pyspark:v18a-q4
  -> eval/results/realstyle/<tag>__realstyle.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "eval"))
import evaluate as ev  # noqa: E402  (also puts sql2pyspark-mlx on sys.path)
from verify_pairs import Checker, rows_of  # noqa: E402

CASES = ROOT / "sql2pyspark-mlx/realstyle_v19/pilot/cases.jsonl"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", required=True)
    ap.add_argument("--set", default=str(CASES))
    ap.add_argument("--think", choices=["true", "false"], default=None)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--num-predict", type=int, default=4096, help="long queries need long replies")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    rows_in = [json.loads(l) for l in Path(args.set).read_text().splitlines() if l.strip()]
    cases = [{"id": r["id"], "domain": r["domain"], **r["case"]} for r in rows_in if r.get("ok") and r.get("case")]
    tag = args.model.split(":", 1)[1] if args.model.startswith("sql2pyspark:") else args.model.replace(":", "_")
    out = Path(args.out) if args.out else ROOT / "eval/results/realstyle" / f"{tag}__realstyle.json"
    out.parent.mkdir(parents=True, exist_ok=True)

    checker = Checker()
    rows, t0 = [], time.time()
    for i, c in enumerate(cases, 1):
        spark = checker.spark.newSession()       # fresh temp views per case
        frames = {}
        for name, tsql in c["tables"].items():
            df = spark.sql(str(tsql).strip().rstrip(";")).cache()
            df.count()
            df.createOrReplaceTempView(name)
            frames[f"df_{name}"] = df
        checker.frames = frames                   # the model's code sees only this case's df_<table>s
        checker.base_ns["spark"] = spark
        sdf = spark.sql(c["sql"].strip().rstrip(";"))
        s_cols, s_rows = sdf.columns, rows_of(sdf)
        try:
            reply, secs = ev.ask(args.model, c["sql"], args.timeout, args.num_predict, None,
                                 None if args.think is None else args.think == "true")
        except Exception as e:  # noqa: BLE001
            reply, secs = f"<request failed: {ev._short(e)}>", 0.0
        code = ev.extract_code(reply)
        order = c.get("order_cols") or []
        rows.append({"id": c["id"], "level": "realstyle", "domain": c["domain"], "sql": c["sql"], "reply": reply,
                     "seconds": round(secs, 2),
                     "strict": ev.score(checker, s_cols, s_rows, code, order),
                     "lenient": ev.score(checker, s_cols, s_rows, ev.lenient(code, c["sql"]), order)})
        for df in frames.values():
            df.unpersist()
        if i % 10 == 0 or i == len(cases):
            ok = sum(r["strict"]["match"] for r in rows)
            print(f"  {i}/{len(cases)}  strict match {ok}/{i}  ({time.time() - t0:.0f}s)", flush=True)
    summary = ev.summarize(rows, "level")
    out.write_text(json.dumps({"model": args.model, "set": args.set, "n": len(rows), "summary": summary,
                               "avg_seconds": round(sum(r["seconds"] for r in rows) / max(1, len(rows)), 2),
                               "cases": rows}, indent=2, ensure_ascii=False))
    a = summary["ALL"]
    print(f"\n{args.model} on real-style pairs (n={a['n']})")
    print(f"  strict : syntax {a['strict_syntax']:.1%}  runs {a['strict_runs']:.1%}  match {a['strict_match']:.1%}")
    print(f"  lenient: syntax {a['lenient_syntax']:.1%}  runs {a['lenient_runs']:.1%}  match {a['lenient_match']:.1%}")
    print(f"  results -> {out}")


if __name__ == "__main__":
    main()
