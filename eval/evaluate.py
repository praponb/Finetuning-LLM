"""Execution-based evaluation of a SQL -> PySpark model served by Ollama.

For each case the model gets the same prompt app.py sends; the ```python block
of the reply is executed against the seeded sample tables and its `result`
DataFrame is compared with running the SQL itself (verify_pairs.compare).

Metrics per case:
  syntax  - the extracted code parses as Python
  runs    - it executes and produces a DataFrame
  match   - same columns + same rows (+ same ORDER BY sequence) as the SQL

Two scoring modes are reported:
  strict  - code is run as-is and must assign `result` (the v2 convention)
  lenient - imports are added, a bare `df` is bound to the query's main table,
            and the last expression / assignment is taken as `result`
            (fair to models trained on the v1 data, which had no such convention)

Usage:
  python eval/evaluate.py --model sql2pyspark:v2 --set eval/holdout.jsonl
  python eval/evaluate.py --model sql2pyspark:v2 --set sql2pyspark-mlx/data/test_meta.jsonl --sample 300
  python eval/evaluate.py --model sql2pyspark:v3 --set eval/real_queries.jsonl --tables eval/real_tables.py
"""
from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "sql2pyspark-mlx"))

from verify_pairs import Checker, Result, _short  # noqa: E402

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/") + "/api/chat"
IMPORTS = "from pyspark.sql import functions as F\nfrom pyspark.sql.window import Window\n"


def ask(model: str, sql: str, timeout: int, num_predict: int) -> tuple[str, float]:
    t0 = time.time()
    r = requests.post(OLLAMA_URL, json={
        "model": model,
        "messages": [{"role": "user", "content": f"Convert SQL to PySpark:\n```sql\n{sql}\n```"}],
        "stream": False,
        "options": {"temperature": 0, "seed": 0, "num_predict": num_predict},
    }, timeout=timeout)
    r.raise_for_status()
    return r.json().get("message", {}).get("content", ""), time.time() - t0


def extract_code(text: str) -> str:
    blocks: list[str] = re.findall(r"```(?:python|pyspark|py)?\s*\n(.*?)```", text, flags=re.S)
    if blocks:
        return max(blocks, key=len).strip()
    # unterminated fence (generation stopped before the closing ```)
    text = re.sub(r"^\s*```(?:python|pyspark|py)?\s*\n", "", text)
    return text.replace("```", "").strip()


def main_table(sql: str) -> str | None:
    m = re.search(r"\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, flags=re.I)
    return m.group(1) if m else None


def lenient(code: str, sql: str) -> str:
    """Make v1-style snippets (no imports, bare `df`, no `result`) runnable."""
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return code
    assigned = {t.id for n in ast.walk(tree) if isinstance(n, ast.Assign) for t in n.targets if isinstance(t, ast.Name)}
    if "result" not in assigned and tree.body:
        last = tree.body[-1]
        if isinstance(last, ast.Expr):
            tree.body[-1] = ast.Assign(targets=[ast.Name("result", ast.Store())], value=last.value, lineno=last.lineno)
        elif isinstance(last, ast.Assign) and isinstance(last.targets[0], ast.Name):
            tree.body.append(ast.parse(f"result = {last.targets[0].id}").body[0])
        code = ast.unparse(ast.fix_missing_locations(tree))
    prefix = IMPORTS
    table = main_table(sql)
    if table and re.search(r"\bdf\b", code) and "df =" not in code:
        prefix += f"df = df_{table}\n"
    return prefix + code


def score(checker: Checker, s_cols, s_rows, code: str, order_cols) -> dict:
    try:
        ast.parse(code)
        syntax = True
    except SyntaxError:
        syntax = False
    res: Result = checker.check_against(s_cols, s_rows, code, order_cols) if syntax else Result(False, "syntax")
    return {"syntax": syntax, "runs": res.status not in ("syntax", "py_error", "no_result"),
            "match": res.ok, "status": res.status, "detail": res.detail[:300]}


def load_tables(path: str, spark) -> dict:
    """Extra sample tables from a .py file that defines make_dataframes(spark) -> {"df_<table>": DataFrame}."""
    spec = importlib.util.spec_from_file_location(Path(path).stem, path)
    assert spec and spec.loader, path
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.make_dataframes(spark)


def load_cases(path: Path, sample: int, seed: int):
    cases = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if sample and sample < len(cases):
        cases = random.Random(seed).sample(cases, sample)
    return cases


def summarize(rows, key):
    groups = defaultdict(list)
    for r in rows:
        groups[r.get(key) or "all"].append(r)
    groups["ALL"] = rows
    out = {}
    for g, rs in sorted(groups.items()):
        n = len(rs)
        out[g] = {"n": n, **{f"{mode}_{m}": round(sum(r[mode][m] for r in rs) / n, 4)
                             for mode in ("strict", "lenient") for m in ("syntax", "runs", "match")}}
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--model", required=True, help="Ollama model tag, e.g. sql2pyspark:v2")
    ap.add_argument("--set", required=True, help="JSONL with sql / order_cols (+ level or family)")
    ap.add_argument("--sample", type=int, default=0, help="random subset size (0 = all)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--timeout", type=int, default=180)
    ap.add_argument("--num-predict", type=int, default=2048,
                    help="max tokens per reply (was 1024 before 2026-09-27; only long real-world queries need more)")
    ap.add_argument("--tables", default=None,
                    help="extra sample tables: a .py file with make_dataframes(spark), e.g. eval/real_tables.py")
    ap.add_argument("--out", default=None, help="results JSON (default eval/results/<model>__<set>.json)")
    ap.add_argument("--reuse-replies", action="store_true",
                    help="re-score the replies already stored in --out instead of querying the model")
    args = ap.parse_args()

    set_path = Path(args.set)
    cases = load_cases(set_path, args.sample, args.seed)
    out = Path(args.out) if args.out else ROOT / "eval" / "results" / f"{args.model.replace(':', '_')}__{set_path.stem}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    cached = {}
    if args.reuse_replies and out.exists():
        cached = {c["id"]: (c["reply"], c["seconds"]) for c in json.loads(out.read_text())["cases"]}
    checker = Checker()
    if args.tables:
        checker.frames.update(load_tables(args.tables, checker.spark))
    rows: list[dict[str, Any]] = []
    t0 = time.time()
    for i, c in enumerate(cases, 1):
        s_cols, s_rows = checker.run_sql(c["sql"])
        if c["id"] in cached:
            reply, secs = cached[c["id"]]
        else:
            try:
                reply, secs = ask(args.model, c["sql"], args.timeout, args.num_predict)
            except Exception as e:  # noqa: BLE001
                reply, secs = f"<request failed: {_short(e)}>", 0.0
        code = extract_code(reply)
        order = c.get("order_cols") or []
        rows.append({
            "id": c["id"], "level": c.get("level") or c.get("family"), "sql": c["sql"], "reply": reply,
            "seconds": round(secs, 2),
            "strict": score(checker, s_cols, s_rows, code, order),
            "lenient": score(checker, s_cols, s_rows, lenient(code, c["sql"]), order),
        })
        if i % 10 == 0 or i == len(cases):
            ok = sum(r["lenient"]["match"] for r in rows)
            print(f"  {i}/{len(cases)}  lenient match {ok}/{i}  ({time.time() - t0:.0f}s)", flush=True)
    summary = summarize(rows, "level")
    out.write_text(json.dumps({"model": args.model, "set": str(set_path), "n": len(rows), "summary": summary,
                               "avg_seconds": round(sum(r["seconds"] for r in rows) / max(1, len(rows)), 2),
                               "cases": rows}, indent=2, ensure_ascii=False))
    a = summary["ALL"]
    print(f"\n{args.model} on {set_path.name} (n={a['n']})")
    print(f"  strict : syntax {a['strict_syntax']:.1%}  runs {a['strict_runs']:.1%}  match {a['strict_match']:.1%}")
    print(f"  lenient: syntax {a['lenient_syntax']:.1%}  runs {a['lenient_runs']:.1%}  match {a['lenient_match']:.1%}")
    print(f"  results -> {out}")


if __name__ == "__main__":
    main()
