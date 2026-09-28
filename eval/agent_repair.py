"""Repair loop: let Kimi K3 fix a model's PySpark answer, with a tool that runs PySpark (kimi_repair.py).

For each query in --set, the agent gets the SQL and the fine-tuned model's answer (from --results, e.g.
eval/results/v5__real_queries.json) and one tool, `run_pyspark`. It never sees the expected output, and code
that calls `.sql(...)` is refused, so it cannot just run the original query. After at most --max-runs tool calls
it gives a final answer, which is scored like evaluate.py against the SQL's result on the sample tables
("before"/"after" are lenient scores; the agent's code is also scored strictly).

--mode sample (default): the tool runs the code on the same sample tables the evaluation uses (eval/real_tables.py)
    and returns the error or the first rows of `result`; the prompt lists the tables' schemas.
--mode empty: what the app does. No data and no schemas: Kimi declares the tables' schemas from the SQL, and the
    tool runs the code on empty tables and compares its columns with the SQL's (errors only, no rows).

Kimi K3 (Moonshot AI) is called through its OpenAI-compatible API. The key is read from KIMI_API_KEY in the
environment or in the repo's .env (gitignored; format in USER_MANUAL.md). --dry-run needs no key: it only runs the
fine-tuned model's answers through the sample-table tool and shows what the agent would see first.

Usage (from the repo root, with sql2pyspark-mlx/.venv):
  python eval/agent_repair.py --results eval/results/v5__real_queries.json --dry-run
  python eval/agent_repair.py --results eval/results/v5__real_queries.json [--mode empty]
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path

import openai

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate import ROOT, Checker, _short, extract_code, lenient, load_cases, load_tables, score  # noqa: E402

sys.path.insert(0, str(ROOT))
import kimi_repair as kr  # noqa: E402

SQL_CALL, MAX_ROWS, MAX_CHARS = kr.SQL_CALL, kr.MAX_ROWS, kr.MAX_CHARS

def table_schemas(checker: Checker, sql: str) -> str:
    names = {m.lower() for m in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql)}
    lines = [f"df_{t[3:]}: {df.schema.simpleString()}" for t, df in sorted(checker.frames.items())
             if t.startswith("df_") and t[3:].lower() in names]
    return "\n".join(lines)


def preview(checker: Checker, code: str) -> str:
    """What the agent sees after a run: the error, or the columns, row count and first rows."""
    if SQL_CALL.search(code):
        return "Refused: the code calls .sql(...). Translate the query to the DataFrame API instead."
    try:
        cols, rows = checker.run_py(code)
    except Exception as e:  # noqa: BLE001
        return f"Error: {_short(e)}"
    out = [f"OK. columns: {cols}", f"rows: {len(rows)}"]
    out += [repr(r) for r in rows[:MAX_ROWS]]
    if len(rows) > MAX_ROWS:
        out.append(f"... {len(rows) - MAX_ROWS} more rows")
    return "\n".join(out)[:MAX_CHARS]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True, help="evaluate.py output with the answers to repair")
    ap.add_argument("--set", default=str(ROOT / "eval" / "real_queries.jsonl"))
    ap.add_argument("--tables", default=str(ROOT / "eval" / "real_tables.py"))
    ap.add_argument("--model", default=None, help="default: KIMI_MODEL from .env, else kimi-k3")
    ap.add_argument("--effort", default="high", choices=["low", "high", "max"], help="Kimi reasoning_effort")
    ap.add_argument("--mode", default="sample", choices=["sample", "empty"],
                    help="tool runs on the sample tables (default) or on empty tables Kimi declares (like the app)")
    ap.add_argument("--max-runs", type=int, default=5, help="run_pyspark calls allowed per query")
    ap.add_argument("--only", default="", help="comma-separated case ids (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="no API calls: show the first tool output per case")
    ap.add_argument("--out", default=None,
                    help="default eval/results/<tag>-repair/<set>[_empty].json (outside report.py's glob)")
    args = ap.parse_args()
    kr.load_env(ROOT / ".env")
    args.model = args.model or os.environ.get("KIMI_MODEL") or "kimi-k3"
    if not args.dry_run and not os.environ.get("KIMI_API_KEY"):
        raise SystemExit("KIMI_API_KEY is empty: put KIMI_API_KEY=... in .env in the repo root (see USER_MANUAL.md)")

    prior = {c["id"]: c for c in json.loads(Path(args.results).read_text())["cases"]}
    cases = [c for c in load_cases(Path(args.set), 0, 0) if c["id"] in prior]
    if args.only:
        keep = set(args.only.split(","))
        cases = [c for c in cases if c["id"] in keep]
    checker = Checker()
    checker.frames.update(load_tables(args.tables, checker.spark))
    client = None if args.dry_run else kr.make_client()

    rows, t0 = [], time.time()
    for c in cases:
        s_cols, s_rows = checker.run_sql(c.get("ref_sql") or c["sql"])
        order = c.get("order_cols") or []
        prior_code = extract_code(prior[c["id"]]["reply"])
        before = score(checker, s_cols, s_rows, lenient(prior_code, c["sql"]), order)
        if args.dry_run:
            print(f"\n=== {c['id']}  before: {before['status']}  {before['detail'][:120]}")
            print(preview(checker, lenient(prior_code, c["sql"]))[:800])
            continue
        print(f"{c['id']}: repairing (before: {before['status']}) ...", flush=True)
        try:
            if args.mode == "sample":
                r = kr.repair(client, args.model, args.effort, c["sql"], prior_code,
                              lambda a: preview(checker, a["code"]) if isinstance(a.get("code"), str)
                              else 'Invalid arguments: expected {"code": "<python code>"}.',
                              args.max_runs, kr.SYSTEM_SAMPLE, kr.TOOL_SAMPLE,
                              context=f"Tables it reads:\n{table_schemas(checker, c['sql'])}\n\n")
            else:   # a separate Spark session, so the empty tables' temp views don't replace the sample tables
                r = kr.repair(client, args.model, args.effort, c["sql"], prior_code,
                              kr.EmptyTableRunner(checker.spark.newSession(), c["sql"]), args.max_runs)
        except openai.APIStatusError as e:
            print(f"  API error {e.status_code}: {e.message}", flush=True)
            raise
        used_sql = bool(SQL_CALL.search(r["code"]))
        after = {"syntax": False, "runs": False, "match": False, "status": "used_sql", "detail": ""} if used_sql \
            else score(checker, s_cols, s_rows, lenient(r["code"], c["sql"]), order)
        strict = score(checker, s_cols, s_rows, r["code"], order)
        rows.append({"id": c["id"], "before": before, "after": after, "after_strict": strict,
                     "n_runs": len(r["runs"]), **r})
        print(f"  after: {after['status']}  runs {len(r['runs'])}  ${r['cost_usd']:.2f}  {r['seconds']:.0f}s"
              f"  {after['detail'][:120]}", flush=True)
    if args.dry_run:
        return

    tag = Path(args.results).stem.split("__")[0]
    suffix = "" if args.mode == "sample" else f"_{args.mode}"
    out = Path(args.out) if args.out else ROOT / "eval" / "results" / f"{tag}-repair" / f"{Path(args.set).stem}{suffix}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    total = {k: sum(r["usage"][k] for r in rows) for k in ("input", "output", "cache_read")}
    summary = {"n": len(rows), "match_before": sum(r["before"]["match"] for r in rows),
               "match_after": sum(r["after"]["match"] for r in rows),
               "runs_after": sum(r["after"]["runs"] for r in rows),
               "tool_runs": sum(r["n_runs"] for r in rows), "usage": total,
               "cost_usd": round(sum(r["cost_usd"] for r in rows), 4), "seconds": round(time.time() - t0, 1)}
    out.write_text(json.dumps({"model": args.model, "effort": args.effort, "mode": args.mode, "max_runs": args.max_runs,
                               "source": args.results, "set": args.set, "summary": summary, "cases": rows},
                              indent=2, ensure_ascii=False))
    print(f"\nrepair with {args.model} ({args.mode} tables, effort {args.effort}, <= {args.max_runs} runs per query)")
    print(f"  match: {summary['match_before']}/{len(rows)} before -> {summary['match_after']}/{len(rows)} after"
          f"  (runs without error after: {summary['runs_after']}/{len(rows)})")
    print(f"  tool runs {summary['tool_runs']}  tokens in {total['input']} ({total['cache_read']} cached)"
          f" / out {total['output']}  cost ${summary['cost_usd']:.2f}")
    print(f"  results -> {out}")


if __name__ == "__main__":
    main()
