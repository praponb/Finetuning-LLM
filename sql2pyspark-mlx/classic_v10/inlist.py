"""Targeted pairs for `x [NOT] IN (SELECT ...)` written WITHOUT table aliases in the SQL (v10).

The classic generator always aliases the outer table in IN-subqueries (`FROM orders o WHERE o.customer_id IN ...`),
so v8/v9 never saw the bare form `SELECT id, name FROM doctors WHERE id NOT IN (SELECT doctor_id FROM appointments)`
and answered it with an unqualified F.col("id") in the join condition (ambiguous: holdout024, holdout052). Here the
SQL has no aliases while the PySpark side aliases both DataFrames and qualifies every column of the join:

    result = (
        df_customers.alias("c")
        .join(df_orders.filter(...).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
        .select(F.col("c.id"), F.col("c.name"))
    )

Forms: parent.id [NOT] IN (SELECT child.fk ...) and child.fk [NOT] IN (SELECT parent.id ...). NOT IN over a
nullable column gets an explicit `IS NOT NULL` in the SQL (as people write it), so left_anti is exact.
Only training tables are used (the hold-out tables never appear).

Usage:  python inlist.py --n 900 --seed 10 --out raw_inlist
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from generate_dataset import TABLES, children, fks, make_alias  # noqa: E402

rng = random.Random(10)


def py_lit(v) -> str:
    return json.dumps(v)


def sql_lit(v) -> str:
    return "'" + str(v).replace("'", "''") + "'" if isinstance(v, str) else str(v)


def pred(t, avoid=()):
    """(sql, py_template) for a simple filter on table t; py_template has {c} for the column reference."""
    cols = [c for c in t.columns if c.name not in avoid and (c.kind in ("cat", "str") and c.pool or c.kind in ("int", "double"))]
    if not cols:
        return None
    c = rng.choice(cols)
    if c.kind in ("cat", "str"):
        if rng.random() < 0.3 and len(c.pool) > 2:
            vals = rng.sample(list(c.pool), 2)
            return (f"{c.name} IN ({', '.join(sql_lit(v) for v in vals)})",
                    c.name, f'.isin({", ".join(py_lit(v) for v in vals)})')
        v = rng.choice(c.pool)
        op = rng.choice(["=", "=", "<>"])
        return f"{c.name} {op} {sql_lit(v)}", c.name, f' {"==" if op == "=" else "!="} {py_lit(v)}'
    lo, hi = c.lo + (c.hi - c.lo) * 0.25, c.hi - (c.hi - c.lo) * 0.25
    v = int(rng.uniform(lo, hi)) if c.kind == "int" else round(rng.uniform(lo, hi), 1)
    op = rng.choice([">", ">=", "<", "<="])
    return f"{c.name} {op} {v}", c.name, f" {op} {v}"


def py_pred(p, qual: str | None) -> str:
    sql, col, tail = p
    ref = f'F.col("{qual}.{col}")' if qual else f'F.col("{col}")'
    return f"{ref}{tail}" if tail.startswith(".") else f"{ref}{tail}"


def build():
    form = rng.choice(["parent_in_child", "child_in_parent"])
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
    used: set = set()
    oa = make_alias(outer.name, used)
    used.add(oa)
    ia = make_alias(inner.name, used)
    if ia == oa:
        return None
    pool = [c for c in outer.columns if c.kind not in ("array", "id") and c.name != outer_key]
    picked = rng.sample(pool, min(len(pool), rng.randint(1, 3)))
    sel = ["id"] + ([outer_key] if outer_key != "id" and rng.random() < 0.5 else []) + [c.name for c in picked]
    sel = list(dict.fromkeys(sel))
    inner_where = [pred(inner, avoid=(inner_key,))] if rng.random() < 0.85 else []
    inner_where = [p for p in inner_where if p]
    outer_where = [pred(outer, avoid=(outer_key,))] if rng.random() < 0.35 else []
    outer_where = [p for p in outer_where if p]
    feats = ["inlist", form, "not_in" if anti else "in"]
    # NOT IN semantics: NULLs in the subquery or in the outer key change the result -> explicit IS NOT NULL
    inner_null_guard = anti and inner_nullable
    outer_null_guard = anti and outer_nullable
    if inner_null_guard:
        feats.append("not_null_inner")
    if outer_null_guard:
        feats.append("not_null_outer")

    # ---- SQL
    up = rng.random() < 0.75
    K = (lambda s: s) if up else (lambda s: s.lower())
    sub_conds = [p[0] for p in inner_where] + ([f"{inner_key} {K('IS NOT NULL')}"] if inner_null_guard else [])
    sub = f"{K('SELECT')} {inner_key} {K('FROM')} {inner.name}" + (
        f" {K('WHERE')} " + f" {K('AND')} ".join(sub_conds) if sub_conds else "")
    conds = [f"{outer_key} {K('NOT IN') if anti else K('IN')} ({sub})"]
    conds += [p[0] for p in outer_where] + ([f"{outer_key} {K('IS NOT NULL')}"] if outer_null_guard else [])
    if rng.random() < 0.5:
        conds = conds[1:] + conds[:1]
    order, desc, limit = None, False, None
    if rng.random() < 0.45:
        order = rng.choice(sel)
        desc = rng.random() < 0.4
        if rng.random() < 0.35:
            limit = rng.choice([3, 5, 10])
            if order != "id":
                order = None if rng.random() < 0.5 else order
                if order is None:
                    limit = None
        feats.append("order_by")
    multiline = rng.random() < 0.5
    sep = "\n" if multiline else " "
    sql = f"{K('SELECT')} {', '.join(sel)}{sep}{K('FROM')} {outer.name}{sep}{K('WHERE')} " + f" {K('AND')} ".join(conds)
    order_sql = None
    if order:
        tie = "" if order == "id" or limit is None else ", id"
        order_sql = f"{order}{' ' + K('DESC') if desc else ''}{tie}"
        sql += f"{sep}{K('ORDER BY')} {order_sql}"
    if limit:
        sql += f"{sep}{K('LIMIT')} {limit}"
    if rng.random() < 0.5:
        sql += ";"

    # ---- PySpark
    inner_df = f"df_{inner.name}"
    inner_filters = [py_pred(p, None) for p in inner_where] + ([f'F.col("{inner_key}").isNotNull()'] if inner_null_guard else [])
    for f in inner_filters:
        inner_df += f".filter({f})"
    cond = f'F.col("{oa}.{outer_key}") == F.col("{ia}.{inner_key}")'
    steps = [f'.join({inner_df}.alias("{ia}"), {cond}, "{"left_anti" if anti else "left_semi"}")']
    outer_filters = [py_pred(p, oa) for p in outer_where] + ([f'F.col("{oa}.{outer_key}").isNotNull()'] if outer_null_guard else [])
    for f in outer_filters:
        steps.append(f".filter({f})")
    sel_py = ", ".join(f'F.col("{oa}.{c}")' for c in sel)
    one = f".select({sel_py})"
    steps.append(one if len(one) <= 90 else ".select(\n" + "".join(f'        F.col("{oa}.{c}"),\n' for c in sel) + "    )")
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
    return sql, code, feats, order_cols


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=900)
    ap.add_argument("--seed", type=int, default=10)
    ap.add_argument("--out", default="raw_inlist")
    args = ap.parse_args()
    rng.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    seen, metas = set(), []
    while len(metas) < args.n:
        res = build()
        if not res:
            continue
        sql, code, feats, order_cols = res
        key = " ".join(sql.lower().split())
        if key in seen:
            continue
        seen.add(key)
        pid = f"inl{len(metas) + 1:05d}"
        (out / f"{pid}.sql").write_text(sql + "\n")
        (out / f"{pid}.py").write_text(code)
        metas.append({"id": pid, "family": "inlist", "features": sorted(set(feats)), "order_cols": order_cols})
    with open(out / "meta_inlist.jsonl", "w") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    print(f"wrote {len(metas)} pairs to {out}")


if __name__ == "__main__":
    main()
