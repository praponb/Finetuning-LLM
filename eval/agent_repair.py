"""Repair loop: let Kimi K3 fix a model's PySpark answer by running it against the sample tables.

For each query in --set, the agent gets the SQL, the schemas of the tables it reads, and the fine-tuned
model's answer (from --results, e.g. eval/results/v5__real_queries.json). It has one tool, `run_pyspark`,
which executes a snippet against the same sample tables the evaluation uses and returns the error or the
first rows of `result`. It never sees the expected output, and code that calls `.sql(...)` is refused,
so it cannot just run the original query. After at most --max-runs tool calls it gives a final answer,
which is scored like evaluate.py against the SQL's result ("before"/"after" are lenient scores; the
agent's code is also scored strictly).

Kimi K3 (Moonshot AI) is called through its OpenAI-compatible API. The key is read from KIMI_API_KEY in
the environment or in the repo's .env (gitignored; template .env.example). --dry-run needs no key: it only
runs the fine-tuned model's answers through the tool and shows what the agent would see first.

Usage (from the repo root, with sql2pyspark-mlx/.venv):
  python eval/agent_repair.py --results eval/results/v5__real_queries.json --dry-run
  python eval/agent_repair.py --results eval/results/v5__real_queries.json
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
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluate import ROOT, Checker, _short, extract_code, lenient, load_cases, load_tables, score  # noqa: E402

# USD per million tokens: (input cache miss, input cache hit, output); Kimi pricing page, 2026-09
PRICES = {"kimi-k3": (3.0, 0.30, 15.0)}
SQL_CALL = re.compile(r"\.sql\s*\(")
MAX_ROWS, MAX_CHARS = 20, 6000

SYSTEM = """You fix PySpark code that was meant to be an exact translation of a Spark SQL query.

Rules for the final code:
- Use the DataFrame API only; never call spark.sql(...) or any .sql(...) method (F.expr is fine).
- Each input table is already bound to a variable named df_<table>; `spark`, `F` (pyspark.sql.functions)
  and `Window` are also defined.
- Assign the final DataFrame to a variable named `result`, with the same columns (names and order) as the
  query's SELECT list. Keep the query's semantics exactly; do not add or drop rows or columns.

You can check your work with the run_pyspark tool, which runs a snippet against small sample tables and
shows the error or the first rows of `result`. You do not know the expected output, so reason about
whether the rows look right for the SQL. The tool has a limited number of runs; use them on the fixes that
matter. When you are done, reply with the complete final code in one ```python block."""


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


def load_env(path: Path) -> None:
    """KEY=VALUE lines from .env into os.environ (values already set in the environment win)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def cost(model: str, u: dict) -> float:
    p_miss, p_hit, p_out = PRICES.get(model, (0.0, 0.0, 0.0))
    return ((u["input"] - u["cache_read"]) * p_miss + u["cache_read"] * p_hit + u["output"] * p_out) / 1e6


TOOLS = [{
    "type": "function",
    "function": {
        "name": "run_pyspark",
        "description": "Run a PySpark snippet against the sample tables and return the error or the first "
                       "rows of `result`.",
        "parameters": {
            "type": "object",
            "properties": {"code": {"type": "string", "description": "Complete Python code; it must assign "
                                                                     "the final DataFrame to `result`."}},
            "required": ["code"],
        },
    },
}]


def repair(client, model: str, effort: str, checker: Checker, case: dict, prior_code: str, max_runs: int) -> dict:
    runs: list[dict] = []

    def run_pyspark(arguments: str) -> str:
        try:
            code = json.loads(arguments)["code"]
            assert isinstance(code, str)
        except (json.JSONDecodeError, KeyError, TypeError, AssertionError):
            return 'Invalid arguments: expected JSON {"code": "<python code>"}.'
        if len(runs) >= max_runs:
            return f"No runs left ({max_runs} used). Reply with your final code now."
        out = preview(checker, code)
        runs.append({"code": code, "output": out[:500]})
        return out + f"\n[run {len(runs)} of {max_runs}]"

    prompt = (f"Spark SQL query:\n```sql\n{case['sql']}\n```\n\n"
              f"Tables it reads:\n{table_schemas(checker, case['sql'])}\n\n"
              f"Current PySpark translation (from a small fine-tuned model; it may be wrong or not run):\n"
              f"```python\n{prior_code}\n```\n\n"
              "Check it with run_pyspark, fix what is wrong, and reply with the complete final code.")
    messages: list = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": prompt}]
    usage = {"input": 0, "output": 0, "cache_read": 0}
    final_text, stop = "", None
    t0 = time.time()
    for _ in range(max_runs + 3):
        # K3 always thinks; temperature/top_p are fixed server-side, so none are sent
        resp = client.chat.completions.create(model=model, messages=messages, tools=TOOLS,
                                              reasoning_effort=effort, max_tokens=32768)
        u = resp.usage
        if u:
            usage["input"] += u.prompt_tokens or 0
            usage["output"] += u.completion_tokens or 0
            details = getattr(u, "prompt_tokens_details", None)
            usage["cache_read"] += (getattr(details, "cached_tokens", None) if details else None) \
                or getattr(u, "cached_tokens", None) or 0
        choice = resp.choices[0]
        msg, stop = choice.message, choice.finish_reason
        # pass the assistant message back unchanged (it carries reasoning_content for the next turn)
        messages.append(msg.model_dump(exclude_none=True))
        if msg.content and msg.content.strip():
            final_text = msg.content
        if not msg.tool_calls:
            break
        for tc in msg.tool_calls:
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": run_pyspark(tc.function.arguments)})
    code = extract_code(final_text) if "```" in final_text else ""
    if not code and runs:          # no code block in the final reply: fall back to the last code it ran
        code = runs[-1]["code"]
    return {"final_reply": final_text, "code": code, "runs": runs, "stop_reason": stop, "usage": usage,
            "cost_usd": round(cost(model, usage), 4), "seconds": round(time.time() - t0, 1)}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True, help="evaluate.py output with the answers to repair")
    ap.add_argument("--set", default=str(ROOT / "eval" / "real_queries.jsonl"))
    ap.add_argument("--tables", default=str(ROOT / "eval" / "real_tables.py"))
    ap.add_argument("--model", default=None, help="default: KIMI_MODEL from .env, else kimi-k3")
    ap.add_argument("--effort", default="high", choices=["low", "high", "max"], help="Kimi reasoning_effort")
    ap.add_argument("--max-runs", type=int, default=5, help="run_pyspark calls allowed per query")
    ap.add_argument("--only", default="", help="comma-separated case ids (default: all)")
    ap.add_argument("--dry-run", action="store_true", help="no API calls: show the first tool output per case")
    ap.add_argument("--out", default=None, help="default eval/results/<tag>-repair/<set>.json (outside report.py's glob)")
    args = ap.parse_args()
    load_env(ROOT / ".env")
    args.model = args.model or os.environ.get("KIMI_MODEL") or "kimi-k3"
    if not args.dry_run and not os.environ.get("KIMI_API_KEY"):
        raise SystemExit("KIMI_API_KEY is empty: put your key in .env (see .env.example)")

    prior = {c["id"]: c for c in json.loads(Path(args.results).read_text())["cases"]}
    cases = [c for c in load_cases(Path(args.set), 0, 0) if c["id"] in prior]
    if args.only:
        keep = set(args.only.split(","))
        cases = [c for c in cases if c["id"] in keep]
    checker = Checker()
    checker.frames.update(load_tables(args.tables, checker.spark))
    client = None if args.dry_run else OpenAI(api_key=os.environ["KIMI_API_KEY"], timeout=900,
                                              base_url=os.environ.get("KIMI_BASE_URL") or "https://api.moonshot.ai/v1")

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
            r = repair(client, args.model, args.effort, checker, c, prior_code, args.max_runs)
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
    out = Path(args.out) if args.out else ROOT / "eval" / "results" / f"{tag}-repair" / f"{Path(args.set).stem}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    total = {k: sum(r["usage"][k] for r in rows) for k in ("input", "output", "cache_read")}
    summary = {"n": len(rows), "match_before": sum(r["before"]["match"] for r in rows),
               "match_after": sum(r["after"]["match"] for r in rows),
               "runs_after": sum(r["after"]["runs"] for r in rows),
               "tool_runs": sum(r["n_runs"] for r in rows), "usage": total,
               "cost_usd": round(sum(r["cost_usd"] for r in rows), 4), "seconds": round(time.time() - t0, 1)}
    out.write_text(json.dumps({"model": args.model, "effort": args.effort, "max_runs": args.max_runs,
                               "source": args.results, "set": args.set, "summary": summary, "cases": rows},
                              indent=2, ensure_ascii=False))
    print(f"\nrepair with {args.model} (effort {args.effort}, <= {args.max_runs} runs per query)")
    print(f"  match: {summary['match_before']}/{len(rows)} before -> {summary['match_after']}/{len(rows)} after"
          f"  (runs without error after: {summary['runs_after']}/{len(rows)})")
    print(f"  tool runs {summary['tool_runs']}  tokens in {total['input']} ({total['cache_read']} cached)"
          f" / out {total['output']}  cost ${summary['cost_usd']:.2f}")
    print(f"  results -> {out}")


if __name__ == "__main__":
    main()
