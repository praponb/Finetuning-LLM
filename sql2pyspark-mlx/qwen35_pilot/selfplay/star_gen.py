"""Fix round for q9df (11-25): `SELECT *` / `SELECT t.*` with [NOT] IN / [NOT] EXISTS subqueries, Spark-verified.

q9d / q9df loop to the token limit on this shape (11-24 app test); the training data has 0 such queries. The forms
reuse classic_v10/inlist.py's tables, predicates and NULL handling (training tables only, the hold-out tables never
appear), with the outer table's columns kept whole:

    SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE city = 'Paris')
    SELECT o.* FROM orders o WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id AND p.status = 'X')
    -> result = (
           df_orders.alias("o")
           .join(df_payments.filter(...).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
           [.filter(...)] [.orderBy(...)] [.limit(n)]
       )                       (semi / anti joins keep exactly the outer columns: no select needed)

NOT IN over a nullable column gets an explicit IS NOT NULL in the SQL (as in inlist.py), so left_anti is exact;
[NOT] EXISTS with an equality correlation is exactly left_semi / left_anti. Every reference is checked in Spark
(verify_pairs.Checker); leakage vs the eval sets, the dev set and the star probe is 0.
  .venv/bin/python qwen35_pilot/selfplay/star_gen.py --n 400 --seed 2041   -> selfplay/star_pool.jsonl (+ stats)
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parents[1]
ROOT = MLX.parent
sys.path.insert(0, str(MLX))
_spec = importlib.util.spec_from_file_location("inlist", MLX / "classic_v10" / "inlist.py")
il = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(il)
from generate_dataset import TABLES, children, fks, make_alias  # noqa: E402

rng = il.rng
norm = lambda s: " ".join(s.split()).lower().rstrip(";").strip()


def build():
    form = rng.choice(["parent_in_child", "child_in_parent"])
    kind = rng.choice(["in", "exists"])
    anti = rng.random() < 0.5
    if form == "parent_in_child":
        parent = rng.choice([t for t in TABLES if children(t)])
        child, fk = rng.choice(children(parent))
        outer, inner, outer_key, inner_key = parent, child, "id", fk.name
        inner_nullable, outer_nullable = fk.nullable, False
    else:
        child = rng.choice([t for t in TABLES if fks(t)])
        fk = rng.choice(fks(child))
        parent = next(t for t in TABLES if t.name == fk.ref)
        outer, inner, outer_key, inner_key = child, parent, fk.name, "id"
        inner_nullable, outer_nullable = False, fk.nullable
    if outer.name == inner.name:
        return None
    used: set = set()
    oa = make_alias(outer.name, used)
    used.add(oa)
    ia = make_alias(inner.name, used)
    if ia == oa:
        return None
    aliased = kind == "exists" or rng.random() < 0.4
    star = f"{oa}.*" if aliased and rng.random() < 0.4 else "*"
    inner_where = [p for p in ([il.pred(inner, avoid=(inner_key,))] if rng.random() < 0.85 else []) if p]
    outer_where = [p for p in ([il.pred(outer, avoid=(outer_key,))] if rng.random() < 0.35 else []) if p]
    feats = ["star", kind, form, ("not_" if anti else "") + kind, "star_alias" if star != "*" else "star_plain"]
    inner_null_guard = kind == "in" and anti and inner_nullable
    outer_null_guard = kind == "in" and anti and outer_nullable

    # ---- SQL
    up = rng.random() < 0.75
    K = (lambda s: s) if up else (lambda s: s.lower())
    q = (lambda a, s: f"{a}.{s}") if aliased else (lambda a, s: s)
    if kind == "in":
        sub_conds = [q(ia, p[0]) for p in inner_where] + ([f"{q(ia, inner_key)} {K('IS NOT NULL')}"] if inner_null_guard else [])
        sub = f"{K('SELECT')} {q(ia, inner_key)} {K('FROM')} {inner.name}{' ' + ia if aliased else ''}" + (
            f" {K('WHERE')} " + f" {K('AND')} ".join(sub_conds) if sub_conds else "")
        main = f"{q(oa, outer_key)} {K('NOT IN') if anti else K('IN')} ({sub})"
    else:
        sub_conds = [f"{ia}.{inner_key} = {oa}.{outer_key}"] + [f"{ia}.{p[0]}" for p in inner_where]
        one = rng.choice(["1", "1", "*"])
        sub = f"{K('SELECT')} {one} {K('FROM')} {inner.name} {ia} {K('WHERE')} " + f" {K('AND')} ".join(sub_conds)
        main = f"{K('NOT EXISTS') if anti else K('EXISTS')} ({sub})"
    conds = [main] + [q(oa, p[0]) for p in outer_where] + ([f"{q(oa, outer_key)} {K('IS NOT NULL')}"] if outer_null_guard else [])
    if rng.random() < 0.5:
        conds = conds[1:] + conds[:1]
    cols = [c.name for c in outer.columns if c.kind != "array"]
    order, desc, limit = None, False, None
    if rng.random() < 0.4:
        order = rng.choice(cols)
        desc = rng.random() < 0.4
        if rng.random() < 0.35:
            limit = rng.choice([3, 5, 10])
        feats.append("order_by")
    sep = "\n" if rng.random() < 0.4 else " "
    sql = (f"{K('SELECT')} {star}{sep}{K('FROM')} {outer.name}{' ' + oa if aliased else ''}{sep}{K('WHERE')} "
           + f" {K('AND')} ".join(conds))
    if order:
        tie = "" if order == "id" or limit is None else f", {q(oa, 'id')}"
        sql += f"{sep}{K('ORDER BY')} {q(oa, order)}{' ' + K('DESC') if desc else ''}{tie}"
    if limit:
        sql += f"{sep}{K('LIMIT')} {limit}"
    if rng.random() < 0.5:
        sql += ";"

    # ---- PySpark
    inner_df = f"df_{inner.name}"
    for f in [il.py_pred(p, None) for p in inner_where] + ([f'F.col("{inner_key}").isNotNull()'] if inner_null_guard else []):
        inner_df += f".filter({f})"
    cond = f'F.col("{oa}.{outer_key}") == F.col("{ia}.{inner_key}")'
    steps = [f'.join({inner_df}.alias("{ia}"), {cond}, "{"left_anti" if anti else "left_semi"}")']
    for f in [il.py_pred(p, oa) for p in outer_where] + ([f'F.col("{oa}.{outer_key}").isNotNull()'] if outer_null_guard else []):
        steps.append(f".filter({f})")
    order_cols = []
    if order:
        o = f'F.col("{order}").desc()' if desc else f'"{order}"'
        tie = "" if order == "id" or limit is None else ', "id"'
        steps.append(f".orderBy({o}{tie})")
        order_cols = [order] + (["id"] if tie else [])
    if limit:
        steps.append(f".limit({limit})")
    code = ("from pyspark.sql import functions as F\n\nresult = (\n"
            f'    df_{outer.name}.alias("{oa}")\n' + "".join(f"    {s}\n" for s in steps) + ")\n")
    return sql, code, sorted(set(feats)), order_cols


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=400)
    ap.add_argument("--seed", type=int, default=2041)
    a = ap.parse_args()
    rng.seed(a.seed)
    sys.path.insert(0, str(HERE))
    from build_pool import eval_sql
    evs = eval_sql() | {norm(json.loads(x)["sql"]) for x in (ROOT / "eval/star_probe.jsonl").read_text().splitlines()}
    system = json.loads((HERE / "pool.jsonl").open().readline())["system"]
    from verify_pairs import Checker
    ck = Checker()
    stats, seen, pool = Counter(), set(), []
    while len(pool) < a.n and stats["tried"] < a.n * 4:
        res = build()
        if not res:
            continue
        sql, code, feats, order_cols = res
        key = norm(sql)
        if key in seen:
            continue
        seen.add(key)
        stats["tried"] += 1
        if key in evs:
            stats["skip_eval_leak"] += 1
            continue
        r = ck.check(sql, code, order_cols)
        if not r.ok:
            stats[f"ref_fail_{r.status}"] += 1
            continue
        try:
            if not ck.run_sql(sql)[1]:
                stats["skip_0_rows"] += 1
                continue
        except Exception:  # noqa: BLE001
            stats["skip_sql_error"] += 1
            continue
        stats["ok"] += 1
        stats.update(feats)
        pool.append({"id": f"st{len(pool):04d}", "source": "star_gen", "sql": sql, "order_cols": order_cols,
                     "reference": f"```python\n{code}```", "system": system, "features": feats})
    (HERE / "star_pool.jsonl").write_text("".join(json.dumps(x) + "\n" for x in pool))
    (HERE / "star_pool_stats.json").write_text(json.dumps(dict(stats), indent=2) + "\n")
    print(json.dumps(dict(stats), indent=2))


if __name__ == "__main__":
    main()
