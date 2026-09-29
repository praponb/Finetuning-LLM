"""Standalone generator of complex SQL -> PySpark pairs (complex_v8 dataset).

Every query is built stage by stage; each stage emits a SQL CTE and the equivalent PySpark block at the
same time, so both sides always describe the same computation. The pairs are then execution-checked in
Spark by verify.py (only pairs whose results match are kept).

Families (targets: long pipelines, many windows, set operations, grouping sets, reshaping):
  pipeline       3-7 mixed stages (joins, windows, EXISTS / NOT EXISTS, scalar subqueries, dedup,
                 aggregation, comparisons to group averages, LATERAL VIEW) + a final stage
  window_heavy   8-16 window functions over 3-6 different window specs (+ a second window stage)
  setop          UNION ALL / UNION / INTERSECT / EXCEPT (+ ALL), positional unions of differently named
                 columns, literal source columns, 3-branch unions, union-then-aggregate
  sessionize     gaps-and-islands sessions (LAG -> flag -> running SUM) + per-session metrics
  interval       non-equi / range joins (coverage periods, tariff windows, self-joins over time)
  full_outer     two aggregates FULL OUTER JOINed on a shared key with COALESCE
  grouping       ROLLUP / CUBE / GROUPING SETS with GROUPING_ID / GROUPING
  reshape        PIVOT / UNPIVOT finals

Output (same layout as the main generator): <out>/<id>.sql, <out>/<id>.py, <out>/meta_complex.jsonl

Usage:  python generate.py --n 11000 --seed 8 --out raw_pairs
"""
from __future__ import annotations

import argparse
import json
import random
import re
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path

from catalog import SINGULAR, TABLE_BY_NAME, TABLES, Col, Table, children_of, values_of

NUM = ("int", "double")
GROUPABLE = ("cat", "fk", "bool")


# ---------------------------------------------------------------------------
# expressions
# ---------------------------------------------------------------------------
@dataclass
class Ref:
    """A column reference / expression in both languages. `arg` is what goes inside F.xxx(...) calls."""
    sql: str
    col: str
    arg: str


def R(name: str, qual: str | None = None) -> Ref:
    full = f"{qual}.{name}" if qual else name
    return Ref(full, f'F.col("{full}")', f'"{full}"')


def Ex(sql: str, py: str) -> Ref:
    return Ref(sql, py, py)


@dataclass
class It:
    """One SELECT item: SQL expression + PySpark Column + output column metadata."""
    sql: str
    py: str
    col: Col
    bare: bool = False

    def s(self) -> str:
        return self.sql if self.bare else f"{self.sql} AS {self.col.name}"

    def p(self) -> str:
        if self.bare:
            return self.py
        py = f"({self.py})" if top_level_op(self.py) else self.py
        return f'{py}.alias("{self.col.name}")'


def top_level_op(py: str) -> bool:
    """True if the expression has a binary operator outside parentheses/strings (a - b.alias() would bind wrong)."""
    depth, in_str = 0, False
    for i, ch in enumerate(py):
        if ch == '"':
            in_str = not in_str
        elif in_str:
            continue
        elif ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif depth == 0 and ch in "+-*/%|&<>=" and 0 < i < len(py) - 1 and py[i - 1] == " ":
            return True
    return False


def bare(c: Col, qual: str | None = None) -> It:
    full = f"{qual}.{c.name}" if qual else c.name
    return It(full, f'"{full}"', c, bare=True)


def renamed(c: Col, new: str, qual: str | None = None) -> It:
    r = R(c.name, qual)
    if new == c.name:
        return bare(c, qual)
    return It(r.sql, r.col, replace(c, name=new))


def sql_lit(v) -> str:
    return "'" + str(v).replace("'", "''") + "'"


def py_lit(v) -> str:
    return json.dumps(v)


def fmt(v, kind: str) -> str:
    if kind == "int":
        return str(int(round(v)))
    v = round(float(v), 1)
    return str(int(v)) + ".0" if v == int(v) else str(v)


def clean(name: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"\W+", "_", name.strip().lower())).strip("_")


# ---------------------------------------------------------------------------
# relations
# ---------------------------------------------------------------------------
@dataclass
class Rel:
    name: str                     # SQL relation name
    py: str                       # PySpark variable
    cols: list                    # list[Col]
    key: list                     # names of columns that identify a row uniquely
    table: str | None = None
    rank_cols: list = field(default_factory=list)   # window ranking outputs usable for top-N filters

    def names(self) -> set:
        return {c.name for c in self.cols}

    def get(self, name: str) -> Col:
        return next(c for c in self.cols if c.name == name)

    def of(self, *kinds) -> list:
        return [c for c in self.cols if c.kind in kinds]


def table_rel(t: Table) -> Rel:
    return Rel(t.name, f"df_{t.name}", [replace(c, src=(t.name, c.name)) for c in t.columns], ["id"], table=t.name)


def fresh(taken: set, base: str) -> str:
    base = clean(base)[:48] or "col"
    name, i = base, 2
    while name in taken:
        name, i = f"{base}_{i}", i + 1
    taken.add(name)
    return name


# ---------------------------------------------------------------------------
# sample values (data-aware thresholds keep most filters non-empty)
# ---------------------------------------------------------------------------
def num_vals(c: Col) -> list:
    if c.src:
        try:
            v = [x for x in values_of(*c.src) if isinstance(x, (int, float)) and not isinstance(x, bool)]
            if v:
                return sorted(v)
        except (KeyError, ValueError):
            pass
    return [c.lo + (c.hi - c.lo) * i / 20 for i in range(21)]


def src_vals(c: Col) -> list:
    if c.src:
        try:
            return sorted(set(values_of(*c.src)), key=str)
        except (KeyError, ValueError):
            pass
    return list(c.pool)


def quant(rng: random.Random, vals: list, lo: float, hi: float):
    n = len(vals)
    return vals[min(n - 1, max(0, int(rng.uniform(lo, hi) * n)))]


def date_vals(c: Col) -> list:
    v = [x for x in src_vals(c) if isinstance(x, (date, datetime))]
    if v:
        return sorted(v)
    if c.kind == "ts":
        return [datetime(2025, 1, 1 + i * 4, 9) for i in range(7)] + [datetime(2025, 2, 1 + i * 4, 9) for i in range(7)]
    return [date(2024, m, 1) for m in range(1, 13)] + [date(2025, m, 1) for m in range(1, 13)]


def ts_lit(v) -> str:
    if isinstance(v, datetime):
        return v.replace(minute=0, second=0).strftime("%Y-%m-%d %H:%M:%S")
    return f"{v.isoformat()} 00:00:00"


def d_lit(v) -> str:
    return (v.date() if isinstance(v, datetime) else v).isoformat()


# ---------------------------------------------------------------------------
# predicates
# ---------------------------------------------------------------------------
def pred(rng: random.Random, c: Col, qual: str | None = None):
    r = R(c.name, qual)
    k, x = c.kind, rng.random()
    if k in NUM:
        vals = num_vals(c)
        if x < 0.15:
            a, b = fmt(quant(rng, vals, .05, .35), k), fmt(quant(rng, vals, .6, .95), k)
            return f"{r.sql} BETWEEN {a} AND {b}", f"{r.col}.between({a}, {b})"
        op = rng.choice([">=", ">", "<=", "<"])
        v = fmt(quant(rng, vals, .1, .45) if op[0] == ">" else quant(rng, vals, .55, .9), k)
        return f"{r.sql} {op} {v}", f"{r.col} {op} {v}"
    if k == "cat":
        pool = [v for v in (src_vals(c) or c.pool) if isinstance(v, str)]
        if not pool:
            return None
        if x < 0.35:
            v = rng.choice(pool)
            return f"{r.sql} = {sql_lit(v)}", f"{r.col} == {py_lit(v)}"
        if x < 0.58 and len(pool) > 2:
            vs = rng.sample(pool, rng.randint(2, min(3, len(pool) - 1)))
            return (f"{r.sql} IN ({', '.join(map(sql_lit, vs))})",
                    f"{r.col}.isin({', '.join(map(py_lit, vs))})")
        if x < 0.72:
            v = rng.choice(pool)
            return f"{r.sql} <> {sql_lit(v)}", f"{r.col} != {py_lit(v)}"
        if x < 0.85 and len(pool) > 2:
            vs = rng.sample(pool, 2)
            return (f"{r.sql} NOT IN ({', '.join(map(sql_lit, vs))})",
                    f"~{r.col}.isin({', '.join(map(py_lit, vs))})")
        return f"{r.sql} IS NOT NULL", f"{r.col}.isNotNull()"
    if k == "str":
        pool = [v for v in (src_vals(c) or c.pool) if isinstance(v, str) and v.strip()]
        if not pool:
            return f"{r.sql} IS NOT NULL", f"{r.col}.isNotNull()"
        v = rng.choice(pool).strip()
        if x < 0.4:
            p = v[0].upper() + "%"
            return f"UPPER(TRIM({r.sql})) LIKE {sql_lit(p)}", f"F.upper(F.trim({r.col})).like({py_lit(p)})"
        if x < 0.6 and len(v) >= 3:
            i = rng.randint(0, len(v) - 2)
            p = "%" + v[i:i + 2].lower() + "%"
            if "'" not in p and "%" not in v:
                return f"LOWER({r.sql}) LIKE {sql_lit(p)}", f"F.lower({r.col}).like({py_lit(p)})"
        if x < 0.8:
            n = rng.randint(4, 8)
            return f"LENGTH(TRIM({r.sql})) >= {n}", f"F.length(F.trim({r.col})) >= {n}"
        return f"{r.sql} IS NOT NULL", f"{r.col}.isNotNull()"
    if k == "date":
        vals = date_vals(c)
        if x < 0.5:
            op = rng.choice([">=", "<"])
            v = d_lit(quant(rng, vals, .15, .5) if op == ">=" else quant(rng, vals, .5, .85))
            return f"{r.sql} {op} DATE '{v}'", f'{r.col} {op} F.lit("{v}").cast("date")'
        if x < 0.7:
            a, b = d_lit(quant(rng, vals, .05, .35)), d_lit(quant(rng, vals, .65, .95))
            return (f"{r.sql} BETWEEN DATE '{a}' AND DATE '{b}'",
                    f'{r.col}.between(F.lit("{a}").cast("date"), F.lit("{b}").cast("date"))')
        if x < 0.85:
            y = quant(rng, vals, .2, .8).year
            return f"YEAR({r.sql}) = {y}", f"F.year({r.col}) == {y}"
        return f"{r.sql} IS NOT NULL", f"{r.col}.isNotNull()"
    if k == "ts":
        vals = date_vals(c)
        if x < 0.5:
            op = rng.choice([">=", "<"])
            v = ts_lit(quant(rng, vals, .15, .5) if op == ">=" else quant(rng, vals, .5, .85))
            return f"{r.sql} {op} TIMESTAMP '{v}'", f'{r.col} {op} F.lit("{v}").cast("timestamp")'
        if x < 0.7:
            a = rng.randint(6, 10)
            b = a + rng.randint(2, 6)
            return f"HOUR({r.sql}) BETWEEN {a} AND {b}", f"F.hour({r.col}).between({a}, {b})"
        if x < 0.85:
            v = d_lit(quant(rng, vals, .4, .9))
            return f"TO_DATE({r.sql}) < DATE '{v}'", f'F.to_date({r.col}) < F.lit("{v}").cast("date")'
        ms = sorted(rng.sample([1, 2, 3], 2))
        return f"MONTH({r.sql}) IN ({ms[0]}, {ms[1]})", f"F.month({r.col}).isin({ms[0]}, {ms[1]})"
    if k == "bool":
        if x < 0.45:
            return f"{r.sql} = TRUE", r.col
        if x < 0.75:
            return f"{r.sql} = FALSE", f"~{r.col}"
        return f"NOT {r.sql}", f"~{r.col}"
    if k == "array":
        pool = list(c.pool)
        if pool and x < 0.6:
            v = rng.choice(pool)
            if x < 0.45:
                return f"ARRAY_CONTAINS({r.sql}, {sql_lit(v)})", f"F.array_contains({r.col}, {py_lit(v)})"
            return f"NOT ARRAY_CONTAINS({r.sql}, {sql_lit(v)})", f"~F.array_contains({r.col}, {py_lit(v)})"
        n = rng.randint(1, 2)
        return f"SIZE({r.sql}) >= {n}", f"F.size({r.col}) >= {n}"
    if k in ("fk", "id"):
        if x < 0.5 or c.hi < 4:
            return f"{r.sql} IS NOT NULL", f"{r.col}.isNotNull()"
        v = rng.randint(2, max(2, int(c.hi) - 1))
        op = rng.choice(["<=", ">="])
        return f"{r.sql} {op} {v}", f"{r.col} {op} {v}"
    return None


def preds_for(rng, cols: list, n: int, qual: str | None = None, allow_or: bool = True) -> list:
    cols = [c for c in cols if c.kind != "id"]
    out = []
    for c in rng.sample(cols, min(n, len(cols))):
        p = pred(rng, c, qual)
        if p:
            out.append(p)
    if allow_or and len(out) >= 2 and rng.random() < 0.2:
        a, b = out[0], out[1]
        out = [(f"({a[0]} OR {b[0]})", f"({a[1]}) | ({b[1]})")] + out[2:]
    return out


def py_and(preds: list) -> str:
    if len(preds) == 1:
        return preds[0][1]
    return " & ".join(f"({p[1]})" for p in preds)


def where_lines(preds: list, kw: str = "WHERE") -> list:
    if not preds:
        return []
    return [f"{kw} {preds[0][0]}"] + [f"  AND {p[0]}" for p in preds[1:]]


# ---------------------------------------------------------------------------
# derived columns
# ---------------------------------------------------------------------------
def num_col(name, kind, lo, hi, nullable=True) -> Col:
    return Col(name, kind, lo=lo, hi=hi, nullable=nullable)


def derive(rng: random.Random, cols: list, taken: set, qual: str | None = None) -> It | None:
    """One derived expression over `cols` (returns None if nothing fits)."""
    cols = [c for c in cols if len(c.name) <= 24]     # keep generated names readable
    nums = [c for c in cols if c.kind in NUM]
    dbls = [c for c in cols if c.kind == "double"]
    strs = [c for c in cols if c.kind in ("cat", "str")]
    cats = [c for c in cols if c.kind == "cat" and len(c.pool) >= 3]
    dates = [c for c in cols if c.kind == "date"]
    tss = [c for c in cols if c.kind == "ts"]
    bools = [c for c in cols if c.kind == "bool"]
    arrs = [c for c in cols if c.kind == "array"]
    ids = [c for c in cols if c.kind in ("id", "fk", "int")]

    def rr(c):
        return R(c.name, qual)

    opts = []
    if dbls:
        def f_mul():
            c = rng.choice(dbls)
            f = rng.choice(["1.07", "0.9", "1.5", "0.25", "1.1"])
            return It(f"ROUND({rr(c).sql} * {f}, 2)", f"F.round({rr(c).col} * {f}, 2)",
                      num_col(fresh(taken, f"{c.name}_adj"), "double", c.lo, c.hi * 1.5))
        opts.append(f_mul)
    if len(nums) >= 2:
        def f_ratio():
            a, b = rng.sample(nums, 2)
            return It(f"ROUND({rr(a).sql} / NULLIF({rr(b).sql}, 0), 3)",
                      f"F.round({rr(a).col} / F.nullif({rr(b).col}, F.lit(0)), 3)",
                      num_col(fresh(taken, f"{a.name}_per_{b.name}"), "double", 0, 10))

        def f_arith():
            a, b = rng.sample(nums, 2)
            op, word = rng.choice([("+", "plus"), ("-", "minus")])
            kind = "int" if a.kind == b.kind == "int" else "double"
            return It(f"{rr(a).sql} {op} {rr(b).sql}", f"{rr(a).col} {op} {rr(b).col}",
                      num_col(fresh(taken, f"{a.name}_{word}_{b.name}"), kind, min(a.lo, b.lo), a.hi + b.hi))

        def f_greatest():
            a, b = rng.sample(nums, 2)
            fn = rng.choice(["GREATEST", "LEAST"])
            kind = "int" if a.kind == b.kind == "int" else "double"
            return It(f"{fn}({rr(a).sql}, {rr(b).sql})", f"F.{fn.lower()}({rr(a).col}, {rr(b).col})",
                      num_col(fresh(taken, f"{fn.lower()}_{a.name}_{b.name}"), kind, min(a.lo, b.lo), max(a.hi, b.hi)))
        opts += [f_ratio, f_arith, f_greatest]
    if nums:
        def f_tier():
            c = rng.choice(nums)
            vals = num_vals(c)
            t1, t2 = sorted([quant(rng, vals, .2, .45), quant(rng, vals, .55, .85)])
            t1, t2 = fmt(t1, c.kind), fmt(t2, c.kind)
            if t1 == t2:
                return None
            labels = rng.choice([("HIGH", "MEDIUM", "LOW"), ("large", "medium", "small"), ("A", "B", "C")])
            r = rr(c)
            return It(f"CASE WHEN {r.sql} >= {t2} THEN '{labels[0]}' WHEN {r.sql} >= {t1} THEN '{labels[1]}' "
                      f"ELSE '{labels[2]}' END",
                      f'F.when({r.col} >= {t2}, "{labels[0]}").when({r.col} >= {t1}, "{labels[1]}")'
                      f'.otherwise("{labels[2]}")',
                      Col(fresh(taken, f"{c.name}_tier"), "cat", pool=labels, nullable=False))

        def f_coalesce():
            c = rng.choice(nums)
            z = "0" if c.kind == "int" else "0.0"
            return It(f"COALESCE({rr(c).sql}, {z})", f"F.coalesce({rr(c).col}, F.lit({z}))",
                      replace(c, name=fresh(taken, f"{c.name}_filled"), nullable=False, src=None))

        def f_bucket():
            c = rng.choice(nums)
            span = max(1.0, c.hi - c.lo)
            step = next((s for s in (1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 10000, 50000, 100000)
                         if span / s <= 8), 1000000)
            if step < 2:
                return None
            return It(f"FLOOR({rr(c).sql} / {step}) * {step}", f"F.floor({rr(c).col} / {step}) * {step}",
                      num_col(fresh(taken, f"{c.name}_bucket"), "int", c.lo, c.hi))

        def f_abs():
            c = rng.choice(nums)
            v = fmt(quant(rng, num_vals(c), .3, .7), c.kind)
            return It(f"ABS({rr(c).sql} - {v})", f"F.abs({rr(c).col} - {v})",
                      num_col(fresh(taken, f"{c.name}_distance"), c.kind, 0, c.hi))
        opts += [f_tier, f_coalesce, f_bucket, f_abs]
    if strs:
        def f_case():
            c = rng.choice(strs)
            r = rr(c)
            fn = rng.choice(["UPPER", "LOWER", "INITCAP"])
            return It(f"{fn}(TRIM({r.sql}))", f"F.{fn.lower()}(F.trim({r.col}))",
                      Col(fresh(taken, f"{c.name}_clean"), "str", pool=c.pool))

        def f_len():
            c = rng.choice(strs)
            return It(f"LENGTH(TRIM({rr(c).sql}))", f"F.length(F.trim({rr(c).col}))",
                      num_col(fresh(taken, f"{c.name}_len"), "int", 2, 14))

        def f_sub():
            c = rng.choice(strs)
            n = rng.randint(2, 4)
            return It(f"SUBSTRING(TRIM({rr(c).sql}), 1, {n})", f"F.substring(F.trim({rr(c).col}), 1, {n})",
                      Col(fresh(taken, f"{c.name}_prefix"), "str"))

        def f_slug():
            c = rng.choice(strs)
            return It(f"LOWER(REGEXP_REPLACE(TRIM({rr(c).sql}), ' ', '_'))",
                      f'F.lower(F.regexp_replace(F.trim({rr(c).col}), " ", "_"))',
                      Col(fresh(taken, f"{c.name}_slug"), "str"))
        opts += [f_case, f_len, f_sub, f_slug]
    if len(strs) >= 2:
        def f_concat_ws():
            a, b = rng.sample(strs, 2)
            sep = rng.choice([" / ", "-", "|", ": "])
            return It(f"CONCAT_WS('{sep}', {rr(a).sql}, {rr(b).sql})",
                      f'F.concat_ws("{sep}", {rr(a).col}, {rr(b).col})',
                      Col(fresh(taken, f"{a.name}_{b.name}_label"), "str"))
        opts.append(f_concat_ws)
    if strs and ids:
        def f_concat():
            a, n = rng.choice(strs), rng.choice(ids)
            return It(f"CONCAT({rr(a).sql}, '#', CAST({rr(n).sql} AS STRING))",
                      f'F.concat({rr(a).col}, F.lit("#"), {rr(n).col}.cast("string"))',
                      Col(fresh(taken, f"{a.name}_ref"), "str"))
        opts.append(f_concat)
    if cats:
        def f_cat_coalesce():
            c = rng.choice(cats)
            return It(f"COALESCE({rr(c).sql}, 'UNKNOWN')", f'F.coalesce({rr(c).col}, F.lit("UNKNOWN"))',
                      Col(fresh(taken, f"{c.name}_or_unknown"), "cat", pool=tuple(c.pool) + ("UNKNOWN",),
                          nullable=False))

        def f_cat_group():
            c = rng.choice(cats)
            pool = [v for v in (src_vals(c) or c.pool) if isinstance(v, str)]
            if len(pool) < 3:
                return None
            g1 = rng.sample(pool, 2)
            rest = [v for v in pool if v not in g1]
            g2 = rng.choice(rest)
            r = rr(c)
            return It(f"CASE WHEN {r.sql} IN ({sql_lit(g1[0])}, {sql_lit(g1[1])}) THEN 'group_a' "
                      f"WHEN {r.sql} = {sql_lit(g2)} THEN 'group_b' ELSE 'other' END",
                      f'F.when({r.col}.isin({py_lit(g1[0])}, {py_lit(g1[1])}), "group_a")'
                      f'.when({r.col} == {py_lit(g2)}, "group_b").otherwise("other")',
                      Col(fresh(taken, f"{c.name}_group"), "cat", pool=("group_a", "group_b", "other"),
                          nullable=False))
        opts += [f_cat_coalesce, f_cat_group]
    if dates or tss:
        def f_part():
            c = rng.choice(dates + tss)
            fn = rng.choice(["YEAR", "MONTH", "QUARTER", "DAYOFWEEK"] + (["HOUR"] if c.kind == "ts" else []))
            rng_ = {"YEAR": (2024, 2026), "MONTH": (1, 12), "QUARTER": (1, 4), "DAYOFWEEK": (1, 7), "HOUR": (6, 16)}
            return It(f"{fn}({rr(c).sql})", f"F.{fn.lower()}({rr(c).col})",
                      num_col(fresh(taken, f"{c.name}_{fn.lower()}"), "int", *rng_[fn]))

        def f_trunc():
            c = rng.choice(dates + tss)
            unit = rng.choice(["MONTH", "QUARTER", "WEEK", "YEAR"] + (["DAY", "HOUR"] if c.kind == "ts" else []))
            return It(f"DATE_TRUNC('{unit}', {rr(c).sql})", f'F.date_trunc("{unit.lower()}", {rr(c).col})',
                      Col(fresh(taken, f"{c.name}_{unit.lower()}"), "ts"))

        def f_fmt():
            c = rng.choice(dates + tss)
            pat = rng.choice(["yyyy-MM", "yyyy-MM-dd", "MMM yyyy"] + (["yyyy-MM-dd HH:00"] if c.kind == "ts" else []))
            return It(f"DATE_FORMAT({rr(c).sql}, '{pat}')", f'F.date_format({rr(c).col}, "{pat}")',
                      Col(fresh(taken, f"{c.name}_label"), "str"))

        def f_since():
            c = rng.choice(dates + tss)
            ref = rng.choice(["2025-12-31", "2026-01-01", "2025-06-30"])
            return It(f"DATEDIFF(DATE '{ref}', {rr(c).sql})",
                      f'F.datediff(F.lit("{ref}").cast("date"), {rr(c).col})',
                      num_col(fresh(taken, f"days_since_{c.name}"), "int", 0, 700))
        opts += [f_part, f_trunc, f_fmt, f_since]
    if dates:
        def f_date_math():
            c = rng.choice(dates)
            r = rr(c)
            x = rng.random()
            if x < 0.35:
                n = rng.choice([7, 30, 90, 365])
                return It(f"DATE_ADD({r.sql}, {n})", f"F.date_add({r.col}, {n})",
                          Col(fresh(taken, f"{c.name}_plus_{n}d"), "date"))
            if x < 0.65:
                n = rng.choice([1, 3, 6, 12])
                return It(f"ADD_MONTHS({r.sql}, {n})", f"F.add_months({r.col}, {n})",
                          Col(fresh(taken, f"{c.name}_plus_{n}m"), "date"))
            if x < 0.85:
                return It(f"LAST_DAY({r.sql})", f"F.last_day({r.col})", Col(fresh(taken, f"{c.name}_month_end"), "date"))
            return It(f"TRUNC({r.sql}, 'MM')", f'F.trunc({r.col}, "MM")',
                      Col(fresh(taken, f"{c.name}_month_start"), "date"))
        opts.append(f_date_math)
    if len(dates) >= 2:
        def f_datediff():
            a, b = rng.sample(dates, 2)
            return It(f"DATEDIFF({rr(a).sql}, {rr(b).sql})", f"F.datediff({rr(a).col}, {rr(b).col})",
                      num_col(fresh(taken, f"days_{b.name}_to_{a.name}"), "int", -700, 700))
        opts.append(f_datediff)
    if tss:
        def f_ts_math():
            c = rng.choice(tss)
            r = rr(c)
            if rng.random() < 0.5:
                return It(f"TO_DATE({r.sql})", f"F.to_date({r.col})", Col(fresh(taken, f"{c.name}_date"), "date"))
            n, unit = rng.choice([(15, "MINUTES"), (30, "MINUTES"), (2, "HOURS"), (1, "DAYS"), (7, "DAYS")])
            return It(f"{r.sql} + INTERVAL {n} {unit}", f'{r.col} + F.expr("INTERVAL {n} {unit}")',
                      Col(fresh(taken, f"{c.name}_plus_{n}{unit[0].lower()}"), "ts"))
        opts.append(f_ts_math)
    if len(tss) >= 2:
        def f_minutes():
            a, b = rng.sample(tss, 2)
            div, unit = rng.choice([(60, "minutes"), (3600, "hours")])
            return It(f"ROUND((UNIX_TIMESTAMP({rr(a).sql}) - UNIX_TIMESTAMP({rr(b).sql})) / {div}, 1)",
                      f"F.round((F.unix_timestamp({rr(a).col}) - F.unix_timestamp({rr(b).col})) / {div}, 1)",
                      num_col(fresh(taken, f"{unit}_{b.name}_to_{a.name}"), "double", -2000, 2000))
        opts.append(f_minutes)
    if bools:
        def f_bool():
            c = rng.choice(bools)
            r = rr(c)
            if rng.random() < 0.5:
                return It(f"CASE WHEN {r.sql} THEN 1 ELSE 0 END", f"F.when({r.col}, 1).otherwise(0)",
                          num_col(fresh(taken, f"{c.name}_flag"), "int", 0, 1, nullable=False))
            return It(f"IF({r.sql}, 'yes', 'no')", f'F.when({r.col}, "yes").otherwise("no")',
                      Col(fresh(taken, f"{c.name}_label"), "cat", pool=("yes", "no"), nullable=False))
        opts.append(f_bool)
    if arrs:
        def f_arr():
            c = rng.choice(arrs)
            r = rr(c)
            x = rng.random()
            if x < 0.4:
                return It(f"SIZE({r.sql})", f"F.size({r.col})", num_col(fresh(taken, f"{c.name}_count"), "int", 0, 3))
            if x < 0.7 and c.pool:
                v = rng.choice(c.pool)
                return It(f"ARRAY_CONTAINS({r.sql}, {sql_lit(v)})", f"F.array_contains({r.col}, {py_lit(v)})",
                          Col(fresh(taken, f"has_{v}"), "bool"))
            return It(f"ARRAY_JOIN(ARRAY_SORT({r.sql}), ',')", f'F.array_join(F.array_sort({r.col}), ",")',
                      Col(fresh(taken, f"{c.name}_text"), "str"))
        opts.append(f_arr)
    rng.shuffle(opts)
    for f in opts:
        snapshot = set(taken)
        it = f()
        if it is not None:
            return it
        taken.clear()
        taken.update(snapshot)
    return None


# ---------------------------------------------------------------------------
# query builder
# ---------------------------------------------------------------------------
class Q:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.ctes: list = []          # (name, comment, sql_lines)
        self.blocks: list = []        # python blocks (strings)
        self.wins: dict = {}          # window definition -> variable
        self.features: list = []
        self.used: set = set()
        self.order_cols: list = []

    def feat(self, *f):
        for x in f:
            if x not in self.features:
                self.features.append(x)

    def name(self, base: str) -> str:
        return fresh(self.used, base)

    def win(self, definition: str, base: str) -> str:
        if definition not in self.wins:
            self.wins[definition] = fresh(self.used, base[:30].rstrip("_"))
        return self.wins[definition]

    def add(self, name: str, comment: str, sql_lines: list, py_block: str):
        self.ctes.append((name, comment, sql_lines))
        self.blocks.append(py_block)

    def render(self, final_sql: list, final_py: str, final_comment: str | None = None):
        parts = []
        if self.ctes:
            chunks = []
            for i, (name, comment, lines) in enumerate(self.ctes):
                body = "\n".join("    " + ln if ln else "" for ln in [f"-- {comment}"] + lines)
                chunks.append(f"{'WITH ' if i == 0 else ''}{name} AS (\n{body}\n)")
            parts.append(",\n".join(chunks))
        if final_comment and self.rng.random() < 0.5:
            parts.append(f"-- {final_comment}")
        parts.append("\n".join(final_sql))
        sql = "\n".join(parts)
        code = ["from pyspark.sql import functions as F"]
        if self.wins:
            code.append("from pyspark.sql.window import Window")
        code.append("")
        if self.wins:
            code += [f"{v} = {d}" for d, v in self.wins.items()]
            code.append("")
        for b in self.blocks:
            code += [b, ""]
        code.append(final_py)
        return sql, "\n".join(code).strip() + "\n"


def sel_sql(items: list, distinct: bool = False) -> list:
    kw = "SELECT DISTINCT" if distinct else "SELECT"
    rendered = [i.s() for i in items]
    if len(rendered) <= 3 and sum(map(len, rendered)) < 70:
        return [f"{kw} {', '.join(rendered)}"]
    return [kw] + [f"    {r}," for r in rendered[:-1]] + [f"    {rendered[-1]}"]


def sel_py(items: list) -> str:
    rendered = [i.p() for i in items]
    if len(rendered) <= 3 and sum(map(len, rendered)) < 70:
        return f".select({', '.join(rendered)})"
    return ".select(\n" + "".join(f"        {r},\n" for r in rendered) + "    )"


def call_py(method: str, args: list) -> str:
    if len(args) <= 3 and sum(map(len, args)) < 70:
        return f".{method}({', '.join(args)})"
    return f".{method}(\n" + "".join(f"        {a},\n" for a in args) + "    )"


def chain(var: str, src: str, steps: list) -> str:
    steps = [s for s in steps if s]
    if not steps:
        return f"{var} = {src}"
    if len(steps) == 1 and "\n" not in steps[0] and len(var) + len(src) + len(steps[0]) < 100:
        return f"{var} = {src}{steps[0]}"
    return f"{var} = (\n    {src}\n" + "".join(f"    {s}\n" for s in steps) + ")"


def star_item() -> It:
    return It("*", '"*"', Col("*", "cat"), bare=True)


# ---------------------------------------------------------------------------
# stages
# ---------------------------------------------------------------------------
CTE_NAMES = {
    "source": ["cleaned", "filtered", "base", "recent", "valid", "scoped", "prepared"],
    "derive": ["enriched", "derived", "labeled", "scored", "tagged", "annotated"],
    "join": ["joined", "with_details", "enriched", "with_parent", "combined_details"],
    "child": ["with_children", "exploded_detail", "detail_rows", "line_level"],
    "exists": ["filtered", "with_activity", "without_matches", "qualified", "screened"],
    "stats": ["with_stats", "with_counts", "profiled", "summarized_rows"],
    "window": ["with_windows", "ranked", "windowed", "sequenced", "with_trends", "metrics"],
    "filter_rank": ["top_rows", "top_per_group", "first_rows", "leaders"],
    "agg": ["summary", "grouped", "per_group", "totals", "stats", "rollup_base"],
    "dedup": ["latest", "deduped", "latest_per_key", "unique_rows"],
    "compare": ["above_average", "vs_group", "compared", "outliers"],
    "explode": ["exploded", "flattened", "per_label"],
}


def cte(q: Q, kind: str, suffix: str | None = None) -> str:
    base = q.rng.choice(CTE_NAMES[kind])
    if suffix and q.rng.random() < 0.5:
        base = f"{base}_{suffix}"
    return q.name(base)


def st_source(q: Q, t: Table, n_derive=(0, 3), n_pred=(1, 2)) -> Rel:
    rng = q.rng
    base = table_rel(t)
    keys = [c for c in base.cols if c.kind in ("id", "fk")]
    others = [c for c in base.cols if c.kind not in ("id", "fk")]
    chosen = set(c.name for c in rng.sample(others, rng.randint(min(3, len(others)), len(others))))
    out = [c for c in base.cols if c in keys or c.name in chosen]
    taken = {c.name for c in base.cols}
    derived = [d for d in (derive(rng, base.cols, taken) for _ in range(rng.randint(*n_derive))) if d]
    preds = preds_for(rng, base.cols, rng.randint(*n_pred)) if rng.random() < 0.9 else []
    name = cte(q, "source", SINGULAR[t.name] + "s" if not SINGULAR[t.name].endswith("s") else t.name)
    use_star = not derived and rng.random() < 0.15
    items = [star_item()] if use_star else [bare(c) for c in out] + derived
    if use_star:
        out = list(base.cols)
    sql = sel_sql(items) + [f"FROM {t.name}"] + where_lines(preds)
    steps = [f".filter({py_and(preds)})" if preds else None, None if use_star else sel_py(items)]
    q.add(name, rng.choice(["Keep valid rows and derive helper columns", f"Filter {t.name}",
                            f"Scope the {t.name} rows"]), sql, chain(name, f"df_{t.name}", steps))
    q.feat("filter" if preds else "no_filter", *[f"derive" for _ in derived[:1]])
    return Rel(name, name, out + [d.col for d in derived], ["id"])


def st_derive(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    taken = set(cur.names())
    derived = [d for d in (derive(rng, cur.cols, taken) for _ in range(rng.randint(1, 3))) if d]
    if not derived:
        return None
    preds = preds_for(rng, cur.cols, 1) if rng.random() < 0.4 else []
    name = cte(q, "derive")
    if rng.random() < 0.6:
        items, out = [star_item()] + derived, list(cur.cols)
    else:
        keep = [c for c in cur.cols if c.name in cur.key or rng.random() < 0.7]
        items, out = [bare(c) for c in keep] + derived, keep
    sql = sel_sql(items) + [f"FROM {cur.name}"] + where_lines(preds)
    if items[0].sql == "*" and len(derived) == 1 and rng.random() < 0.6:
        d = derived[0]
        steps = [f".filter({py_and(preds)})" if preds else None, f'.withColumn("{d.col.name}", {d.py})']
    else:
        steps = [f".filter({py_and(preds)})" if preds else None, sel_py(items)]
    q.add(name, rng.choice(["Derive helper columns", "Add calculated columns", "Label and score rows"]),
          sql, chain(name, cur.py, steps))
    q.feat("derive")
    return Rel(name, name, out + [d.col for d in derived], cur.key, rank_cols=cur.rank_cols)


def alias_for(t: str, used: set) -> str:
    for cand in (t[0], t[:2], SINGULAR[t][:3], SINGULAR[t]):
        if cand not in used and cand not in ("as", "on", "by", "or", "in", "is", "to"):
            used.add(cand)
            return cand
    return fresh(used, t[:3])


def prefixed(prefix: str, name: str, taken: set) -> str:
    base = name if (name.startswith(prefix + "_") and name not in taken) else f"{prefix}_{name}"
    return fresh(taken, base)


def st_join(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    fks = [c for c in cur.cols if c.kind == "fk" and c.ref in TABLE_BY_NAME]
    if not fks:
        return None
    fk = rng.choice(fks)
    P = TABLE_BY_NAME[fk.ref]
    used = {"s"}
    pa = alias_for(P.name, used)
    how = rng.choice(["inner", "inner", "left"])
    taken = set(cur.names())
    pcols = [c for c in P.columns if c.kind not in ("id",)]
    pick = rng.sample(pcols, rng.randint(1, min(3, len(pcols))))
    new_items = []
    for c in pick:
        nc = replace(c, src=(P.name, c.name), nullable=True if how == "left" else c.nullable)
        new_items.append(renamed(nc, prefixed(SINGULAR[P.name], c.name, taken), pa))
    on = [(f"{pa}.id = s.{fk.name}", f'F.col("{pa}.id") == F.col("s.{fk.name}")')]
    extra = [replace(c, src=(P.name, c.name)) for c in pcols if c.kind != "fk"]
    if extra and rng.random() < 0.15:
        p = pred(rng, rng.choice(extra), pa)
        if p:
            on.append(p)
    joins_sql = [f"{'LEFT ' if how == 'left' else ''}JOIN {P.name} {pa} ON " + " AND ".join(o[0] for o in on)]
    joins_py = [f'.join(df_{P.name}.alias("{pa}"), {py_and(on)}, "{how}")']
    q.feat(f"join_{how}")
    # optional second join: grandparent through the parent, or another parent of cur
    if rng.random() < 0.35:
        cands = [(pa, c) for c in P.columns if c.kind == "fk" and c.ref in TABLE_BY_NAME] + \
                [("s", c) for c in fks if c is not fk and c.ref != P.name]
        if cands:
            via, gfk = rng.choice(cands)
            G = TABLE_BY_NAME[gfk.ref]
            ga = alias_for(G.name, used)
            how2 = rng.choice(["inner", "left", "left"])
            gcols = [c for c in G.columns if c.kind not in ("id", "fk")]
            for c in rng.sample(gcols, rng.randint(1, min(2, len(gcols)))):
                nc = replace(c, src=(G.name, c.name), nullable=True)
                new_items.append(renamed(nc, prefixed(SINGULAR[G.name], c.name, taken), ga))
            joins_sql.append(f"{'LEFT ' if how2 == 'left' else ''}JOIN {G.name} {ga} ON {ga}.id = {via}.{gfk.name}")
            joins_py.append(f'.join(df_{G.name}.alias("{ga}"), F.col("{ga}.id") == F.col("{via}.{gfk.name}"), "{how2}")')
            q.feat("multi_join")
    wpreds = []
    if how == "inner" and rng.random() < 0.3:
        c = rng.choice(pick)
        p = pred(rng, replace(c, src=(P.name, c.name)), pa)
        if p:
            wpreds.append(p)
    star = rng.random() < 0.25
    keep = list(cur.cols) if star else [c for c in cur.cols if c.name in cur.key or c.kind == "fk" or rng.random() < 0.75]
    items = ([It("s.*", '"s.*"', Col("*", "cat"), bare=True)] if star else [bare(c, "s") for c in keep]) + new_items
    name = cte(q, "join", SINGULAR[P.name])
    sql = sel_sql(items) + [f"FROM {cur.name} s"] + joins_sql + where_lines(wpreds)
    steps = joins_py + ([f".filter({py_and(wpreds)})"] if wpreds else []) + [sel_py(items)]
    q.add(name, rng.choice([f"Attach {SINGULAR[P.name]} attributes", f"Join {P.name}", "Enrich with dimension data"]),
          sql, chain(name, f'{cur.py}.alias("s")', steps))
    return Rel(name, name, keep + [i.col for i in new_items], cur.key, rank_cols=[r for r in cur.rank_cols if r in {c.name for c in keep}])


def ref_cols(cur: Rel) -> list:
    """(column, child table, child fk) for every key column of cur whose table has children."""
    out = []
    for c in cur.cols:
        if c.kind in ("id", "fk") and c.ref:
            for t, fk in children_of(c.ref):
                if t.name != cur.table:
                    out.append((c, t, fk))
    return out


def st_join_child(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    cands = [x for x in ref_cols(cur) if x[0].kind == "id"] or ref_cols(cur)
    if not cands:
        return None
    c, C, fk = rng.choice(cands)
    ca = alias_for(C.name, {"s"})
    how = rng.choice(["inner", "inner", "left"])
    taken = set(cur.names())
    prefix = SINGULAR[C.name]
    id_item = renamed(replace(C.col("id"), src=(C.name, "id"), kind="fk", ref=C.name, nullable=how == "left"),
                      fresh(taken, f"{prefix}_id"), ca)
    ccols = [x for x in C.columns if x.kind not in ("id",) and x.name != fk.name]
    pick = rng.sample(ccols, rng.randint(2, min(4, len(ccols))))
    new = [id_item] + [renamed(replace(x, src=(C.name, x.name), nullable=True if how == "left" else x.nullable),
                               prefixed(prefix, x.name, taken), ca) for x in pick]
    on = [(f"{ca}.{fk.name} = s.{c.name}", f'F.col("{ca}.{fk.name}") == F.col("s.{c.name}")')]
    if rng.random() < 0.3:
        pc = rng.choice(pick)
        p = pred(rng, replace(pc, src=(C.name, pc.name)), ca)
        if p:
            on.append(p)
    keep = [x for x in cur.cols if x.name in cur.key or x.kind in ("fk",) or rng.random() < 0.6]
    items = [bare(x, "s") for x in keep] + new
    name = cte(q, "child", prefix + "s")
    sql = sel_sql(items) + [f"FROM {cur.name} s", f"{'LEFT ' if how == 'left' else ''}JOIN {C.name} {ca} ON " +
                            " AND ".join(o[0] for o in on)]
    steps = [f'.join(df_{C.name}.alias("{ca}"), {py_and(on)}, "{how}")', sel_py(items)]
    q.add(name, f"One row per {prefix}", sql, chain(name, f'{cur.py}.alias("s")', steps))
    q.feat(f"join_child_{how}")
    return Rel(name, name, keep + [i.col for i in new], cur.key + [id_item.col.name])


def st_exists(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    x = rng.random()
    name = cte(q, "exists")
    if x < 0.7 and ref_cols(cur):
        c, C, fk = rng.choice(ref_cols(cur))
        ca = alias_for(C.name, {"s"})
        neg = rng.random() < 0.45
        cc = [replace(y, src=(C.name, y.name)) for y in C.columns if y.kind not in ("id",) and y.name != fk.name]
        # NOT EXISTS without a selective inner filter removes nearly every row on this data
        inner = preds_for(rng, cc, rng.randint(1, 2) if neg else rng.randint(0, 2), ca, allow_or=False)
        form = "in" if (not neg and rng.random() < 0.3) else "exists"
        if form == "in":
            sub = [f"s.{c.name} IN (", f"        SELECT {ca}.{fk.name}", f"        FROM {C.name} {ca}"] + \
                  [f"        {ln}" for ln in where_lines(inner)] + ["    )"]
        else:
            sub = [f"{'NOT ' if neg else ''}EXISTS (", "        SELECT 1", f"        FROM {C.name} {ca}",
                   f"        WHERE {ca}.{fk.name} = s.{c.name}"] + [f"          AND {p[0]}" for p in inner] + ["    )"]
        outer = preds_for(rng, cur.cols, 1, "s", allow_or=False) if rng.random() < 0.3 else []
        sql = ["SELECT s.*", f"FROM {cur.name} s", "WHERE " + sub[0]] + sub[1:] + [f"  AND {p[0]}" for p in outer]
        on = [(None, f'F.col("{ca}.{fk.name}") == F.col("s.{c.name}")')] + inner
        steps = [f'.join(df_{C.name}.alias("{ca}"), {py_and(on)}, "{"left_anti" if neg else "left_semi"}")']
        if outer:
            unq = [(None, p[1].replace('F.col("s.', 'F.col("')) for p in outer]
            steps.append(f".filter({py_and(unq)})")
        q.add(name, "Rows with no matching " + C.name if neg else f"Rows that have matching {C.name}",
              sql, chain(name, f'{cur.py}.alias("s")', steps))
        q.feat("not_exists" if neg else ("in_subquery" if form == "in" else "exists"))
        return Rel(name, name, list(cur.cols), cur.key, rank_cols=cur.rank_cols)
    fks = [c for c in cur.cols if c.kind == "fk" and c.ref in TABLE_BY_NAME]
    if x < 0.85 and fks:
        fk = rng.choice(fks)
        P = TABLE_BY_NAME[fk.ref]
        pa = alias_for(P.name, {"s"})
        pc = [replace(y, src=(P.name, y.name)) for y in P.columns if y.kind not in ("id", "fk")]
        inner = preds_for(rng, pc, rng.randint(1, 2), pa, allow_or=False)
        if not inner:
            return None
        neg = rng.random() < 0.35
        sql = ["SELECT s.*", f"FROM {cur.name} s", f"WHERE {'NOT ' if neg else ''}EXISTS (", "        SELECT 1",
               f"        FROM {P.name} {pa}", f"        WHERE {pa}.id = s.{fk.name}"] + \
              [f"          AND {p[0]}" for p in inner] + ["    )"]
        on = [(None, f'F.col("{pa}.id") == F.col("s.{fk.name}")')] + inner
        q.add(name, f"Keep rows whose {SINGULAR[P.name]} matches", sql,
              chain(name, f'{cur.py}.alias("s")',
                    [f'.join(df_{P.name}.alias("{pa}"), {py_and(on)}, "{"left_anti" if neg else "left_semi"}")']))
        q.feat("not_exists_parent" if neg else "exists_parent")
        return Rel(name, name, list(cur.cols), cur.key, rank_cols=cur.rank_cols)
    nums = cur.of(*NUM)
    if not nums:
        return None
    m = rng.choice(nums)
    fn = rng.choice(["AVG", "AVG", "MAX", "MIN"])
    op = ">" if fn in ("AVG", "MIN") else "<"
    if fn == "MIN":
        op = ">"
    helper = fresh(set(cur.names()), f"overall_{fn.lower()}_{m.name}")
    sql = ["SELECT *", f"FROM {cur.name}", f"WHERE {m.name} {op} (SELECT {fn}({m.name}) FROM {cur.name})"]
    steps = [f'.crossJoin({cur.py}.agg(F.{fn.lower()}("{m.name}").alias("{helper}")))',
             f'.filter(F.col("{m.name}") {op} F.col("{helper}"))', f'.drop("{helper}")']
    q.add(name, f"Compare against the overall {fn.lower()}", sql, chain(name, cur.py, steps))
    q.feat("scalar_subquery_filter")
    return Rel(name, name, list(cur.cols), cur.key, rank_cols=cur.rank_cols)


@dataclass
class Metric:
    sql: str        # aggregate over the child table, unqualified (for a GROUP BY CTE)
    qsql: str       # same aggregate with alias-qualified columns (for a correlated scalar subquery)
    py: str
    col: Col
    fill_zero: bool


def child_metric(rng, C: Table, taken: set, a: str) -> Metric:
    """One aggregate over the child table's rows."""
    prefix = SINGULAR[C.name]
    nums = [c for c in C.columns if c.kind in NUM]
    temps = [c for c in C.columns if c.kind in ("date", "ts")]
    cats = [c for c in C.columns if c.kind in ("cat", "bool")]

    def unq(s: str) -> str:
        return s.replace(f"{a}.", "").replace(f'F.col("{a}.', 'F.col("')

    def count():
        return Metric("COUNT(*)", "COUNT(*)", 'F.count("*")', num_col(fresh(taken, f"{prefix}_count"), "int", 0, 8, False), True)

    x = rng.random()
    if x < 0.3 or not (nums or temps):
        return count()
    if x < 0.45 and cats:
        c = rng.choice(cats)
        p = pred(rng, replace(c, src=(C.name, c.name)), a)
        if p:
            q_agg = f"COUNT(*) FILTER (WHERE {p[0]})" if rng.random() < 0.5 else f"COUNT(CASE WHEN {p[0]} THEN 1 END)"
            return Metric(unq(q_agg), q_agg, f"F.count(F.when({unq(p[1])}, 1))",
                          num_col(fresh(taken, f"{prefix}_{clean(c.name)}_count"), "int", 0, 8, False), True)
    if x < 0.7 and nums:
        c = rng.choice(nums)
        fn = rng.choice(["SUM", "AVG", "MAX", "MIN"])
        name = fresh(taken, f"total_{prefix}_{c.name}" if fn == "SUM" else f"{fn.lower()}_{prefix}_{c.name}")
        if fn == "AVG":
            return Metric(f"ROUND(AVG({c.name}), 2)", f"ROUND(AVG({a}.{c.name}), 2)", f'F.round(F.avg("{c.name}"), 2)',
                          num_col(name, "double", c.lo, c.hi), False)
        return Metric(f"{fn}({c.name})", f"{fn}({a}.{c.name})", f'F.{fn.lower()}("{c.name}")',
                      num_col(name, c.kind, c.lo, c.hi * (6 if fn == "SUM" else 1)), False)
    if temps:
        c = rng.choice(temps)
        fn = rng.choice(["MAX", "MIN"])
        word = "last" if fn == "MAX" else "first"
        name = fresh(taken, f"{word}_{c.name}" if c.name.startswith(prefix) else f"{word}_{prefix}_{c.name}")
        return Metric(f"{fn}({c.name})", f"{fn}({a}.{c.name})", f'F.{fn.lower()}("{c.name}")', Col(name, c.kind), False)
    return count()


def st_child_stats(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    cands = ref_cols(cur)
    if not cands:
        return None
    c, C, fk = rng.choice(cands)
    ca = alias_for(C.name, {"s"})
    taken = set(cur.names())
    metrics: list[Metric] = []
    for _ in range(rng.randint(1, 3)):
        metrics.append(child_metric(rng, C, taken, ca))
    stats_var = q.name(f"{SINGULAR[C.name]}_stats")
    name = cte(q, "stats")
    stats_block = chain(stats_var, f"df_{C.name}",
                        [f'.groupBy("{fk.name}")', call_py("agg", [f'{m.py}.alias("{m.col.name}")' for m in metrics])])
    if rng.random() < 0.5:
        # correlated scalar subqueries in SQL == one pre-aggregated LEFT JOIN in PySpark
        sel = ["s.*"] + [f"(SELECT {m.qsql} FROM {C.name} {ca} WHERE {ca}.{fk.name} = s.{c.name}) AS {m.col.name}"
                         for m in metrics]
        sql = ["SELECT"] + [f"    {x}," for x in sel[:-1]] + [f"    {sel[-1]}", f"FROM {cur.name} s"]
        q.feat("scalar_subquery_select")
        block_prefix = stats_block + "\n\n"
    else:
        q.ctes.append((stats_var, f"Per-{SINGULAR[c.ref]} {C.name} metrics",
                       sel_sql([bare(fk)] + [It(m.sql, "", m.col) for m in metrics]) +
                       [f"FROM {C.name}", f"GROUP BY {fk.name}"]))
        q.blocks.append(stats_block)
        sel = ["s.*"] + [f"COALESCE({ca}.{m.col.name}, 0) AS {m.col.name}" if m.fill_zero else f"{ca}.{m.col.name}"
                         for m in metrics]
        sql = ["SELECT"] + [f"    {x}," for x in sel[:-1]] + [f"    {sel[-1]}", f"FROM {cur.name} s",
                                                              f"LEFT JOIN {stats_var} {ca} ON {ca}.{fk.name} = s.{c.name}"]
        q.feat("agg_cte_left_join")
        block_prefix = ""
    items_py = [It("s.*", '"s.*"', Col("*", "cat"), bare=True)] + \
        [It("", f'F.coalesce(F.col("{ca}.{m.col.name}"), F.lit(0))', m.col) if m.fill_zero else
         It("", f'"{ca}.{m.col.name}"', m.col, bare=True) for m in metrics]
    block = chain(name, f'{cur.py}.alias("s")',
                  [f'.join({stats_var}.alias("{ca}"), F.col("{ca}.{fk.name}") == F.col("s.{c.name}"), "left")',
                   sel_py(items_py)])
    q.add(name, f"Attach {C.name} metrics", sql, block_prefix + block)
    return Rel(name, name, list(cur.cols) + [m.col for m in metrics], cur.key, rank_cols=cur.rank_cols)


# ---- windows -------------------------------------------------------------
@dataclass
class WSpec:
    part_sql: list
    part_py: list
    ord_sql: list
    ord_py: list
    frame_sql: str = ""
    frame_py: str = ""
    hint: str = "w"

    def sql(self) -> str:
        s = []
        if self.part_sql:
            s.append("PARTITION BY " + ", ".join(self.part_sql))
        if self.ord_sql:
            s.append("ORDER BY " + ", ".join(self.ord_sql))
        if self.frame_sql:
            s.append(self.frame_sql)
        return " ".join(s)

    def py(self) -> str:
        s = "Window"
        s += f".partitionBy({', '.join(self.part_py)})" if self.part_py else ""
        s += f".orderBy({', '.join(self.ord_py)})" if self.ord_py else ""
        s += self.frame_py
        return s

    def with_frame(self, fsql: str, fpy: str, suffix: str) -> "WSpec":
        return replace(self, frame_sql=fsql, frame_py=fpy, hint=f"{self.hint}_{suffix}")


FRAMES = {
    "running": ("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                ".rowsBetween(Window.unboundedPreceding, Window.currentRow)"),
    "full": ("ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
             ".rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)"),
}


def moving(n: int):
    return f"ROWS BETWEEN {n} PRECEDING AND CURRENT ROW", f".rowsBetween(-{n}, Window.currentRow)"


def prior(n: int):
    return f"ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING", f".rowsBetween(-{n}, -1)"


def part_candidates(rng, cur: Rel) -> list:
    out = []
    for c in cur.cols:
        if c.kind in ("cat", "fk", "bool") or (c.kind == "int" and c.hi - c.lo <= 12):
            out.append((c.name, f'"{c.name}"', c.name))
        elif c.kind in ("date", "ts") and rng.random() < 0.5:
            fn = rng.choice(["YEAR", "MONTH", "DATE_TRUNC_MONTH"] + (["TO_DATE"] if c.kind == "ts" else []))
            if fn == "DATE_TRUNC_MONTH":
                out.append((f"DATE_TRUNC('MONTH', {c.name})", f'F.date_trunc("month", "{c.name}")', f"{c.name}_month"))
            else:
                out.append((f"{fn}({c.name})", f'F.{fn.lower()}("{c.name}")', f"{c.name}_{fn.lower()}"))
    return out


def order_candidates(cur: Rel) -> list:
    return [c for c in cur.cols if c.kind in ("int", "double", "date", "ts") and c.name not in cur.key]


def make_spec(rng, cur: Rel, ordered: bool, allow_no_part: bool = False) -> WSpec | None:
    parts = part_candidates(rng, cur)
    n_part = 0 if (allow_no_part and rng.random() < 0.3) or not parts else rng.randint(1, min(2, len(parts)))
    chosen = rng.sample(parts, n_part)
    spec = WSpec([p[0] for p in chosen], [p[1] for p in chosen], [], [],
                 hint="w_" + "_".join(clean(p[2]) for p in chosen) if chosen else "w_all")
    if not ordered:
        return spec if chosen else None
    ords = order_candidates(cur)
    if not ords:
        return None
    o = rng.choice(ords)
    desc = rng.random() < 0.4
    nulls = rng.random() < 0.15
    osql = f"{o.name} {'DESC' if desc else 'ASC' if rng.random() < 0.3 else ''}".strip()
    if nulls:
        osql += " NULLS LAST" if not desc else " NULLS FIRST"
        opy = f'F.col("{o.name}").{"desc_nulls_first" if desc else "asc_nulls_last"}()'
    else:
        opy = f'F.col("{o.name}").desc()' if desc else f'"{o.name}"'
    spec.ord_sql, spec.ord_py = [osql], [opy]
    for k in cur.key:
        if k != o.name:
            spec.ord_sql.append(k)
            spec.ord_py.append(f'"{k}"')
    spec.hint = (spec.hint + "_by_" + clean(o.name))[:40]
    return spec


def window_items(q: Q, cur: Rel, n: int, specs: int) -> tuple[list, list, list]:
    """n window expressions over up to `specs` different window specs -> (items, named, rank_cols)."""
    rng = q.rng
    taken = set(cur.names())
    ordered_specs = [s for s in (make_spec(rng, cur, True, allow_no_part=cur.table is None and len(cur.key) <= 3)
                                 for _ in range(specs)) if s]
    part_specs = [s for s in (make_spec(rng, cur, False) for _ in range(max(1, specs // 2))) if s]
    nums = [c for c in cur.of(*NUM) if len(c.name) <= 28]
    vals = nums + [c for c in cur.of("date", "ts") if len(c.name) <= 28]
    items, ranks = [], []
    named = {}
    if not ordered_specs and not part_specs:
        return [], [], []

    def over(spec: WSpec, frame_key: str | None = None):
        sp = spec
        if frame_key:
            fs, fp = FRAMES[frame_key] if frame_key in FRAMES else frame_key
            sp = spec.with_frame(fs, fp, frame_key if isinstance(frame_key, str) and frame_key in FRAMES else "frame")
        var = q.win(sp.py(), sp.hint)
        return sp, var

    def fmt_over(sp: WSpec, var: str):
        if not sp.frame_sql and rng.random() < 0.3:
            named[var] = sp.sql()
            return f"OVER {var}"
        return f"OVER ({sp.sql()})"

    for _ in range(n * 3):
        if len(items) >= n:
            break
        x = rng.random()
        if ordered_specs and x < 0.62:
            spec = rng.choice(ordered_specs)
            o = spec.ord_sql[0].split()[0]
            kind = rng.choice(["row_number", "rank", "dense_rank", "ntile", "percent_rank", "cume_dist", "lag",
                               "lead", "delta", "running", "moving", "prior", "first_last", "ignore_nulls", "nth"])
            if kind in ("row_number", "rank", "dense_rank", "ntile", "percent_rank", "cume_dist"):
                if kind in ("rank", "dense_rank", "percent_rank", "cume_dist") and len(spec.ord_sql) > 1 \
                        and rng.random() < 0.6:
                    # tie-aware functions are deterministic without the unique tie-breaker
                    spec = replace(spec, ord_sql=spec.ord_sql[:1], ord_py=spec.ord_py[:1], hint=spec.hint + "_ties")
                sp, var = over(spec)
                if kind == "ntile":
                    k = rng.choice([3, 4, 5, 10])
                    s_fn, p_fn, nm = f"NTILE({k})", f"F.ntile({k})", {3: "tercile", 4: "quartile", 5: "quintile", 10: "decile"}[k]
                    nm = f"{o}_{nm}"
                elif kind in ("percent_rank", "cume_dist"):
                    s_fn, p_fn, nm = f"{kind.upper()}()", f"F.{kind}()", f"{o}_{kind}"
                else:
                    s_fn, p_fn = f"{kind.upper()}()", f"F.{kind}()"
                    nm = rng.choice(["seq_no", "rn", f"{o}_seq"]) if kind == "row_number" else f"{o}_{kind}"
                name = fresh(taken, nm)
                if kind in ("percent_rank", "cume_dist"):
                    items.append(It(f"ROUND({s_fn} {fmt_over(sp, var)}, 3)", f"F.round({p_fn}.over({var}), 3)",
                                    num_col(name, "double", 0, 1)))
                else:
                    items.append(It(f"{s_fn} {fmt_over(sp, var)}", f"{p_fn}.over({var})", num_col(name, "int", 1, 10, False)))
                    if kind != "ntile":
                        ranks.append(name)
            elif kind in ("lag", "lead") and vals:
                v = rng.choice(vals)
                sp, var = over(spec)
                off = rng.choice([1, 1, 1, 2, 3])
                dflt = v.kind in NUM and rng.random() < 0.25
                a_sql = f"{v.name}" + (f", {off}" if off != 1 or dflt else "") + (", 0" if dflt else "")
                a_py = f'"{v.name}"' + (f", {off}" if off != 1 or dflt else "") + (", 0" if dflt else "")
                pre = ("prev" if kind == "lag" else "next") + (str(off) if off > 1 else "")
                name = fresh(taken, f"{pre}_{v.name}")
                items.append(It(f"{kind.upper()}({a_sql}) {fmt_over(sp, var)}", f"F.{kind}({a_py}).over({var})",
                                replace(v, name=name, nullable=True)))
            elif kind == "delta" and vals:
                v = rng.choice(vals)
                sp, var = over(spec)
                o_ = fmt_over(sp, var)
                if v.kind in NUM:
                    items.append(It(f"{v.name} - LAG({v.name}) {o_}", f'F.col("{v.name}") - F.lag("{v.name}").over({var})',
                                    num_col(fresh(taken, f"{v.name}_change"), v.kind, -v.hi, v.hi)))
                elif v.kind == "date":
                    items.append(It(f"DATEDIFF({v.name}, LAG({v.name}) {o_})",
                                    f'F.datediff(F.col("{v.name}"), F.lag("{v.name}").over({var}))',
                                    num_col(fresh(taken, f"days_since_prev_{v.name}"), "int", 0, 200)))
                else:
                    items.append(It(f"ROUND((UNIX_TIMESTAMP({v.name}) - UNIX_TIMESTAMP(LAG({v.name}) {o_})) / 60, 1)",
                                    f'F.round((F.unix_timestamp("{v.name}") - F.unix_timestamp(F.lag("{v.name}").over({var}))) / 60, 1)',
                                    num_col(fresh(taken, f"minutes_since_prev_{v.name}"), "double", 0, 5000)))
            elif kind == "running" and nums:
                v = rng.choice(nums)
                sp, var = over(spec, "running")
                fn = rng.choice(["SUM", "SUM", "MAX", "MIN", "COUNT"])
                if fn == "COUNT":
                    items.append(It(f"COUNT(*) OVER ({sp.sql()})", f"F.count(F.lit(1)).over({var})",
                                    num_col(fresh(taken, "running_count"), "int", 1, 20, False)))
                else:
                    nm = {"SUM": f"running_{v.name}", "MAX": f"max_{v.name}_so_far", "MIN": f"min_{v.name}_so_far"}[fn]
                    items.append(It(f"{fn}({v.name}) OVER ({sp.sql()})", f'F.{fn.lower()}("{v.name}").over({var})',
                                    num_col(fresh(taken, nm), v.kind, v.lo, v.hi * 10)))
            elif kind in ("moving", "prior") and nums:
                v = rng.choice(nums)
                nprev = rng.choice([1, 2, 3, 6])
                fsql, fpy = moving(nprev) if kind == "moving" else prior(nprev)
                sp = spec.with_frame(fsql, fpy, f"{kind}_{nprev}")
                var = q.win(sp.py(), sp.hint)
                fn = rng.choice(["AVG", "AVG", "SUM", "MAX"])
                nm = f"{v.name}_{fn.lower()}_{nprev + 1 if kind == 'moving' else nprev}" + ("_prior" if kind == "prior" else "")
                if fn == "AVG":
                    items.append(It(f"ROUND(AVG({v.name}) OVER ({sp.sql()}), 2)", f'F.round(F.avg("{v.name}").over({var}), 2)',
                                    num_col(fresh(taken, nm), "double", v.lo, v.hi)))
                else:
                    items.append(It(f"{fn}({v.name}) OVER ({sp.sql()})", f'F.{fn.lower()}("{v.name}").over({var})',
                                    num_col(fresh(taken, nm), v.kind, v.lo, v.hi * 3)))
            elif kind == "first_last" and vals:
                v = rng.choice(vals)
                sp, var = over(spec, "full")
                fn = rng.choice(["FIRST_VALUE", "LAST_VALUE"])
                items.append(It(f"{fn}({v.name}) OVER ({sp.sql()})",
                                f'F.{"first" if fn == "FIRST_VALUE" else "last"}("{v.name}").over({var})',
                                replace(v, name=fresh(taken, f"{'first' if fn == 'FIRST_VALUE' else 'last'}_{v.name}"))))
            elif kind == "ignore_nulls" and vals:
                v = rng.choice(vals)
                sp, var = over(spec, "running")
                fn = rng.choice(["FIRST_VALUE", "LAST_VALUE"])
                nm = f"first_known_{v.name}" if fn == "FIRST_VALUE" else f"last_known_{v.name}"
                items.append(It(f"{fn}({v.name}) IGNORE NULLS OVER ({sp.sql()})",
                                f'F.{"first" if fn == "FIRST_VALUE" else "last"}("{v.name}", ignorenulls=True).over({var})',
                                replace(v, name=fresh(taken, nm))))
            elif kind == "nth" and vals:
                v = rng.choice(vals)
                sp, var = over(spec, "full")
                items.append(It(f"NTH_VALUE({v.name}, 2) OVER ({sp.sql()})", f'F.nth_value("{v.name}", 2).over({var})',
                                replace(v, name=fresh(taken, f"second_{v.name}"))))
        elif nums and (part_specs or ordered_specs):
            spec = rng.choice(part_specs) if part_specs else replace(rng.choice(ordered_specs), ord_sql=[], ord_py=[])
            if not spec.part_sql:
                continue
            sp = spec
            var = q.win(sp.py(), sp.hint)
            v = rng.choice(nums)
            p = clean(spec.hint[2:].split("_by_")[0]) or "all"
            kind = rng.choice(["agg", "agg", "share", "deviation", "count", "distinct"])
            if kind == "agg":
                fn = rng.choice(["SUM", "AVG", "MAX", "MIN"])
                if fn == "AVG":
                    items.append(It(f"ROUND(AVG({v.name}) {fmt_over(sp, var)}, 2)", f'F.round(F.avg("{v.name}").over({var}), 2)',
                                    num_col(fresh(taken, f"{p}_avg_{v.name}"), "double", v.lo, v.hi)))
                else:
                    items.append(It(f"{fn}({v.name}) {fmt_over(sp, var)}", f'F.{fn.lower()}("{v.name}").over({var})',
                                    num_col(fresh(taken, f"{p}_{fn.lower()}_{v.name}"), v.kind, v.lo, v.hi * 10)))
            elif kind == "share":
                items.append(It(f"ROUND({v.name} / NULLIF(SUM({v.name}) {fmt_over(sp, var)}, 0), 4)",
                                f'F.round(F.col("{v.name}") / F.nullif(F.sum("{v.name}").over({var}), F.lit(0)), 4)',
                                num_col(fresh(taken, f"{v.name}_share_of_{p}"), "double", 0, 1)))
            elif kind == "deviation":
                items.append(It(f"ROUND({v.name} - AVG({v.name}) {fmt_over(sp, var)}, 2)",
                                f'F.round(F.col("{v.name}") - F.avg("{v.name}").over({var}), 2)',
                                num_col(fresh(taken, f"{v.name}_vs_{p}_avg"), "double", -v.hi, v.hi)))
            elif kind == "count":
                items.append(It(f"COUNT(*) {fmt_over(sp, var)}", f"F.count(F.lit(1)).over({var})",
                                num_col(fresh(taken, f"rows_in_{p}"), "int", 1, 20, False)))
            else:
                cs = [c for c in cur.cols if c.kind in ("cat", "fk") and c.name not in spec.part_sql]
                if not cs:
                    continue
                c = rng.choice(cs)
                items.append(It(f"SIZE(COLLECT_SET({c.name}) {fmt_over(sp, var)})",
                                f'F.size(F.collect_set("{c.name}").over({var}))',
                                num_col(fresh(taken, f"distinct_{c.name}_in_{p}"), "int", 1, 10, False)))
    return items, named, ranks


def st_windows(q: Q, cur: Rel, n=(2, 6), specs=(1, 3)) -> Rel | None:
    rng = q.rng
    items, named, ranks = window_items(q, cur, rng.randint(*n), rng.randint(*specs))
    seen, uniq = set(), []
    for i in items:                    # no duplicate window expressions under two names
        if i.py not in seen:
            seen.add(i.py)
            uniq.append(i)
    items = uniq
    ranks = [r for r in ranks if r in {i.col.name for i in items}]
    if not items:
        return None
    name = cte(q, "window")
    if rng.random() < 0.55:
        sel, out = [star_item()] + items, list(cur.cols)
    else:
        keep = [c for c in cur.cols if c.name in cur.key or rng.random() < 0.7]
        sel, out = [bare(c) for c in keep] + items, keep
    sql = sel_sql(sel) + [f"FROM {cur.name}"]
    if named:
        sql.append("WINDOW " + ", ".join(f"{k} AS ({v})" for k, v in named.items()))
    if sel[0].sql == "*" and len(items) <= 2 and rng.random() < 0.5:
        steps = [f'.withColumn("{i.col.name}", {i.py})' for i in items]
    else:
        steps = [sel_py(sel)]
    q.add(name, rng.choice(["Window metrics over several partitions", "Running and moving aggregates",
                            "Rank and compare rows within groups", "Previous / next values per key"]),
          sql, chain(name, cur.py, steps))
    q.feat("windows" if len(items) < 6 else "many_windows", *(["named_window"] if named else []))
    return Rel(name, name, out + [i.col for i in items], cur.key,
               rank_cols=[r for r in cur.rank_cols if r in {c.name for c in out}] + ranks)


def st_filter_rank(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    if not cur.rank_cols:
        return None
    r = rng.choice(cur.rank_cols)
    k = rng.choice([1, 1, 2, 3, 5])
    p = (f"{r} = 1", f'F.col("{r}") == 1') if k == 1 else (f"{r} <= {k}", f'F.col("{r}") <= {k}')
    preds = [p] + (preds_for(rng, [c for c in cur.cols if c.name != r], 1) if rng.random() < 0.3 else [])
    name = cte(q, "filter_rank")
    drop = rng.random() < 0.5
    if drop:
        keep = [c for c in cur.cols if c.name != r]
        sql = sel_sql([bare(c) for c in keep]) + [f"FROM {cur.name}"] + where_lines(preds)
        steps = [f".filter({py_and(preds)})", f'.drop("{r}")']
    else:
        keep = list(cur.cols)
        sql = ["SELECT *", f"FROM {cur.name}"] + where_lines(preds)
        steps = [f".filter({py_and(preds)})"]
    q.add(name, f"Top {k} per group" if k > 1 else "First row per group", sql, chain(name, cur.py, steps))
    q.feat("top_n_filter")
    return Rel(name, name, keep, cur.key, rank_cols=[x for x in cur.rank_cols if x != r])


def st_dedup(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    parts = [c for c in cur.cols if c.kind in ("fk", "cat") and c.name not in cur.key]
    ords = [c for c in cur.cols if c.kind in ("ts", "date", "int", "double")]
    if not parts or not ords:
        return None
    p = rng.sample(parts, rng.randint(1, min(2, len(parts))))
    o = rng.choice(ords)
    desc = rng.random() < 0.75
    ord_sql = [f"{o.name} DESC" if desc else o.name] + [f"{k} DESC" if desc else k for k in cur.key]
    ord_py = [f'F.col("{o.name}").desc()' if desc else f'"{o.name}"'] + \
             [f'F.col("{k}").desc()' if desc else f'"{k}"' for k in cur.key]
    spec = WSpec([c.name for c in p], [f'"{c.name}"' for c in p], ord_sql, ord_py,
                 hint="w_dedup" if rng.random() < 0.5 else "w_latest")
    var = q.win(spec.py(), spec.hint)
    rn = fresh(set(cur.names()), rng.choice(["rn", "row_num", "recency_rank"]))
    name = cte(q, "dedup")
    sql = sel_sql([bare(c) for c in cur.cols]) + ["FROM (", "    SELECT", "        *,",
                                                   f"        ROW_NUMBER() OVER ({spec.sql()}) AS {rn}",
                                                   f"    FROM {cur.name}", ") ranked", f"WHERE {rn} = 1"]
    steps = [f'.withColumn("{rn}", F.row_number().over({var}))', f'.filter(F.col("{rn}") == 1)', f'.drop("{rn}")']
    q.add(name, f"Keep the {'latest' if desc else 'earliest'} row per {', '.join(c.name for c in p)}", sql,
          chain(name, cur.py, steps))
    q.feat("dedup_row_number")
    return Rel(name, name, list(cur.cols), cur.key, rank_cols=cur.rank_cols)


# ---- aggregation ---------------------------------------------------------
def group_key_items(rng, cur: Rel, taken: set, n: int) -> list:
    cands = []
    for c in cur.cols:
        if c.kind in ("cat", "fk", "bool") or (c.kind == "int" and c.hi - c.lo <= 12):
            cands.append(("col", c))
        elif c.kind in ("date", "ts"):
            cands.append(("time", c))
        elif c.kind in NUM and rng.random() < 0.3:
            cands.append(("tier", c))
    rng.shuffle(cands)
    items = []
    for how, c in cands:
        if len(items) >= n:
            break
        if how == "col":
            items.append(bare(c))
        elif how == "time":
            x = rng.random()
            if x < 0.45:
                items.append(It(f"DATE_TRUNC('MONTH', {c.name})", f'F.date_trunc("month", "{c.name}")',
                                Col(fresh(taken, f"{c.name}_month"), "ts")))
            elif x < 0.65:
                items.append(It(f"YEAR({c.name})", f'F.year("{c.name}")', num_col(fresh(taken, f"{c.name}_year"), "int", 2024, 2026)))
            elif x < 0.85 and c.kind == "ts":
                items.append(It(f"TO_DATE({c.name})", f'F.to_date("{c.name}")', Col(fresh(taken, f"{c.name}_day"), "date")))
            else:
                items.append(It(f"DATE_FORMAT({c.name}, 'yyyy-MM')", f'F.date_format("{c.name}", "yyyy-MM")',
                                Col(fresh(taken, f"{c.name}_ym"), "str")))
        else:
            d = derive(random.Random(rng.random()), [c], taken)
            if d and d.col.kind == "cat":
                items.append(d)
    return items


def agg_metrics(rng, cur: Rel, taken: set, n: int) -> list:
    """list of (sql, py, Col)."""
    nums = cur.of(*NUM)
    temps = cur.of("date", "ts")
    cats = [c for c in cur.cols if c.kind in ("cat", "bool", "str") and c.src]
    ents = [c for c in cur.cols if c.kind in ("fk", "id", "cat")]
    out = []
    for _ in range(n * 3):
        if len(out) >= n:
            break
        x = rng.random()
        if x < 0.15 and not any(o[0] == "COUNT(*)" for o in out):
            out.append(("COUNT(*)", 'F.count("*")', num_col(fresh(taken, rng.choice(["n_rows", "row_count", "cnt"])), "int", 1, 12, False)))
        elif x < 0.25 and ents:
            c = rng.choice(ents)
            out.append((f"COUNT(DISTINCT {c.name})", f'F.countDistinct("{c.name}")',
                        num_col(fresh(taken, f"distinct_{c.name}s" if not c.name.endswith("s") else f"distinct_{c.name}"), "int", 1, 10, False)))
        elif x < 0.5 and nums:
            c = rng.choice(nums)
            fn = rng.choice(["SUM", "AVG", "MAX", "MIN", "SUM"])
            if fn == "AVG":
                out.append((f"ROUND(AVG({c.name}), 2)", f'F.round(F.avg("{c.name}"), 2)',
                            num_col(fresh(taken, f"avg_{c.name}"), "double", c.lo, c.hi)))
            elif fn == "SUM" and c.kind == "double" and rng.random() < 0.5:
                out.append((f"ROUND(SUM({c.name}), 2)", f'F.round(F.sum("{c.name}"), 2)',
                            num_col(fresh(taken, f"total_{c.name}"), "double", c.lo, c.hi * 6)))
            else:
                nm = {"SUM": "total", "MAX": "max", "MIN": "min"}[fn]
                out.append((f"{fn}({c.name})", f'F.{fn.lower()}("{c.name}")',
                            num_col(fresh(taken, f"{nm}_{c.name}"), c.kind, c.lo, c.hi * (6 if fn == "SUM" else 1))))
        elif x < 0.68 and cats:
            c = rng.choice(cats)
            p = pred(rng, c)
            if not p:
                continue
            y = rng.random()
            tag = clean(re.sub(r"'[^']*'", lambda m: m.group(0).strip("'"), p[0]))[:28]
            if y < 0.3:
                out.append((f"COUNT(*) FILTER (WHERE {p[0]})", f"F.count(F.when({p[1]}, 1))",
                            num_col(fresh(taken, f"n_{tag}"), "int", 0, 10, False)))
            elif y < 0.5:
                out.append((f"SUM(CASE WHEN {p[0]} THEN 1 ELSE 0 END)", f"F.sum(F.when({p[1]}, 1).otherwise(0))",
                            num_col(fresh(taken, f"n_{tag}"), "int", 0, 10)))
            elif y < 0.7:
                out.append((f"ROUND(100 * COUNT(CASE WHEN {p[0]} THEN 1 END) / COUNT(*), 1)",
                            f'F.round(100 * F.count(F.when({p[1]}, 1)) / F.count("*"), 1)',
                            num_col(fresh(taken, f"pct_{tag}"), "double", 0, 100)))
            elif nums:
                m = rng.choice(nums)
                if rng.random() < 0.5:
                    out.append((f"SUM(CASE WHEN {p[0]} THEN {m.name} ELSE 0 END)",
                                f'F.sum(F.when({p[1]}, F.col("{m.name}")).otherwise(0))',
                                num_col(fresh(taken, f"{m.name}_{tag}"), m.kind, 0, m.hi * 5)))
                else:
                    out.append((f"SUM({m.name}) FILTER (WHERE {p[0]})", f'F.sum(F.when({p[1]}, F.col("{m.name}")))',
                                num_col(fresh(taken, f"{m.name}_{tag}"), m.kind, 0, m.hi * 5)))
        elif x < 0.78 and temps:
            c = rng.choice(temps)
            fn = rng.choice(["MAX", "MIN"])
            if c.kind == "date" and rng.random() < 0.3:
                out.append((f"DATEDIFF(MAX({c.name}), MIN({c.name}))", f'F.datediff(F.max("{c.name}"), F.min("{c.name}"))',
                            num_col(fresh(taken, f"{c.name}_span_days"), "int", 0, 700)))
            else:
                out.append((f"{fn}({c.name})", f'F.{fn.lower()}("{c.name}")',
                            replace(c, name=fresh(taken, f"{'last' if fn == 'MAX' else 'first'}_{c.name}"))))
        elif x < 0.86 and cats:
            c = rng.choice([c for c in cats if c.kind != "bool"] or cats)
            if c.kind == "bool":
                continue
            out.append((f"ARRAY_SORT(COLLECT_SET({c.name}))", f'F.array_sort(F.collect_set("{c.name}"))',
                        Col(fresh(taken, f"{c.name}_list"), "array")))
        elif x < 0.93 and nums:
            c = rng.choice(nums)
            if rng.random() < 0.5:
                out.append((f"PERCENTILE_APPROX({c.name}, 0.5)", f'F.percentile_approx("{c.name}", 0.5)',
                            replace(c, name=fresh(taken, f"median_{c.name}"), src=None)))
            else:
                out.append((f"ROUND(STDDEV({c.name}), 2)", f'F.round(F.stddev("{c.name}"), 2)',
                            num_col(fresh(taken, f"stddev_{c.name}"), "double", 0, c.hi)))
        elif len(nums) >= 2:
            a, b = rng.sample(nums, 2)
            out.append((f"ROUND(SUM({a.name}) / NULLIF(SUM({b.name}), 0), 3)",
                        f'F.round(F.sum("{a.name}") / F.nullif(F.sum("{b.name}"), F.lit(0)), 3)',
                        num_col(fresh(taken, f"{a.name}_to_{b.name}_ratio"), "double", 0, 10)))
    if not out:
        out.append(("COUNT(*)", 'F.count("*")', num_col(fresh(taken, "n_rows"), "int", 1, 12, False)))
    seen, uniq = set(), []
    for m in out:                      # no duplicate aggregates under two names
        if m[0] not in seen:
            seen.add(m[0])
            uniq.append(m)
    return uniq


def st_aggregate(q: Q, cur: Rel, having: bool = True) -> Rel | None:
    rng = q.rng
    taken = set(cur.names())
    keys = group_key_items(rng, cur, taken, rng.randint(1, 3))
    if not keys:
        return None
    taken |= {k.col.name for k in keys}
    mets = agg_metrics(rng, cur, taken, rng.randint(2, 6))
    preds = preds_for(rng, cur.cols, 1) if rng.random() < 0.3 else []
    items = keys + [It(m[0], m[1], m[2]) for m in mets]
    if rng.random() < 0.3:
        gb = "GROUP BY " + ", ".join(str(i + 1) for i in range(len(keys)))
    else:
        gb = "GROUP BY " + ", ".join(k.sql for k in keys)
    hv_sql, hv_steps = [], []
    if having and rng.random() < 0.4:
        cnt = next((m for m in mets if m[0] == "COUNT(*)"), None)
        if cnt and rng.random() < 0.6:
            k = rng.choice([2, 2, 3])
            hv_sql = [f"HAVING COUNT(*) >= {k}"]
            hv_steps = [f'.filter(F.col("{cnt[2].name}") >= {k})']
        else:
            nums = cur.of(*NUM)
            if nums:
                c = rng.choice(nums)
                v = fmt(quant(rng, num_vals(c), .1, .4), c.kind)
                fn = rng.choice(["AVG", "MAX", "SUM"])
                same = next((m for m in mets if m[0] in (f"{fn}({c.name})", f"ROUND({fn}({c.name}), 2)")), None)
                if same and same[0] == f"{fn}({c.name})":
                    hv_sql = [f"HAVING {fn}({c.name}) > {v}"]
                    hv_steps = [f'.filter(F.col("{same[2].name}") > {v})']
                else:
                    helper = fresh(taken, f"{fn.lower()}_{c.name}_check")
                    hv_sql = [f"HAVING {fn}({c.name}) > {v}"]
                    items_py_extra = f'F.{fn.lower()}("{c.name}").alias("{helper}")'
                    hv_steps = ["__EXTRA__" + items_py_extra, f'.filter(F.col("{helper}") > {v})', f'.drop("{helper}")']
    name = cte(q, "agg")
    sql = sel_sql(items) + [f"FROM {cur.name}"] + where_lines(preds) + [gb] + hv_sql
    agg_args = [f'{m[1]}.alias("{m[2].name}")' for m in mets]
    if hv_steps and hv_steps[0].startswith("__EXTRA__"):
        agg_args.append(hv_steps[0][len("__EXTRA__"):])
        hv_steps = hv_steps[1:]
    steps = ([f".filter({py_and(preds)})"] if preds else []) + \
        [call_py("groupBy", [k.p() for k in keys]), call_py("agg", agg_args)] + hv_steps
    q.add(name, rng.choice(["Aggregate with conditional metrics", "Summarise per group", "Group-level totals"]),
          sql, chain(name, cur.py, steps))
    q.feat("group_by", *(["having"] if hv_sql else []))
    return Rel(name, name, [k.col for k in keys] + [m[2] for m in mets], [k.col.name for k in keys])


def st_compare_group(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    keys = [c for c in cur.cols if c.kind in ("cat", "fk") and c.name not in cur.key]
    nums = cur.of(*NUM)
    if not keys or not nums:
        return None
    k, m = rng.choice(keys), rng.choice(nums)
    taken = set(cur.names())
    fn = rng.choice(["AVG", "AVG", "MAX", "MEDIAN"])
    stat = fresh(taken, {"AVG": f"avg_{m.name}", "MAX": f"max_{m.name}", "MEDIAN": f"median_{m.name}"}[fn])
    gname = q.name(f"{clean(k.name)}_stats")
    s_sql = {"AVG": f"AVG({m.name})", "MAX": f"MAX({m.name})", "MEDIAN": f"PERCENTILE_APPROX({m.name}, 0.5)"}[fn]
    s_py = {"AVG": f'F.avg("{m.name}")', "MAX": f'F.max("{m.name}")', "MEDIAN": f'F.percentile_approx("{m.name}", 0.5)'}[fn]
    q.add(gname, f"{fn.title()} {m.name} per {k.name}", [f"SELECT {k.name}, {s_sql} AS {stat}", f"FROM {cur.name}",
                                                          f"GROUP BY {k.name}"],
          chain(gname, cur.py, [f'.groupBy("{k.name}")', f'.agg({s_py}.alias("{stat}"))']))
    name = cte(q, "compare")
    op = {"AVG": ">", "MAX": "=", "MEDIAN": ">="}[fn]
    diff = fresh(taken, f"{m.name}_minus_{fn.lower()}")
    items = [It("s.*", '"s.*"', Col("*", "cat"), bare=True), It(f"g.{stat}", f'"g.{stat}"', num_col(stat, "double", m.lo, m.hi), bare=True),
             It(f"ROUND(s.{m.name} - g.{stat}, 2)", f'F.round(F.col("s.{m.name}") - F.col("g.{stat}"), 2)',
                num_col(diff, "double", -m.hi, m.hi))]
    sql = sel_sql(items) + [f"FROM {cur.name} s", f"JOIN {gname} g ON g.{k.name} = s.{k.name}",
                            f"WHERE s.{m.name} {op} g.{stat}"]
    pop = "==" if op == "=" else op
    steps = [f'.join({gname}.alias("g"), F.col("g.{k.name}") == F.col("s.{k.name}"), "inner")',
             f'.filter(F.col("s.{m.name}") {pop} F.col("g.{stat}"))', sel_py(items)]
    q.add(name, "Rows compared with their group statistic", sql, chain(name, f'{cur.py}.alias("s")', steps))
    q.feat("compare_to_group")
    return Rel(name, name, list(cur.cols) + [items[1].col, items[2].col], cur.key)


def st_explode(q: Q, cur: Rel) -> Rel | None:
    rng = q.rng
    arrs = cur.of("array")
    if not arrs:
        return None
    a = rng.choice(arrs)
    taken = set(cur.names())
    single = a.name[:-1] if a.name.endswith("s") else f"{a.name}_item"
    el = fresh(taken, single)
    outer = rng.random() < 0.35
    pos = rng.random() < 0.25
    keep = [c for c in cur.cols if c.name != a.name]
    name = cte(q, "explode")
    if pos:
        pn = fresh(taken, f"{single}_pos")
        sql = sel_sql([bare(c, "s") for c in keep] + [It(f"t.{pn}", "", num_col(pn, "int", 0, 3), bare=True),
                                                      It(f"t.{el}", "", Col(el, "cat", pool=a.pool), bare=True)])
        sql += [f"FROM {cur.name} s", f"LATERAL VIEW {'OUTER ' if outer else ''}POSEXPLODE(s.{a.name}) t AS {pn}, {el}"]
        fn = "posexplode_outer" if outer else "posexplode"
        items = [bare(c) for c in keep] + [It("", f'F.{fn}("{a.name}").alias("{pn}", "{el}")', Col(el, "cat"), bare=True)]
        py = chain(name, cur.py, [sel_py(items)])
        out = keep + [num_col(pn, "int", 0, 3), Col(el, "cat", pool=a.pool)]
    else:
        sql = sel_sql([bare(c, "s") for c in keep] + [It(f"t.{el}", "", Col(el, "cat", pool=a.pool), bare=True)])
        sql += [f"FROM {cur.name} s", f"LATERAL VIEW {'OUTER ' if outer else ''}EXPLODE(s.{a.name}) t AS {el}"]
        items = [bare(c) for c in keep] + [It("", f'F.{"explode_outer" if outer else "explode"}("{a.name}")',
                                              Col(el, "cat", pool=a.pool))]
        py = chain(name, cur.py, [sel_py(items)])
        out = keep + [Col(el, "cat", pool=a.pool)]
    q.add(name, f"One row per {single}", sql, py)
    q.feat("lateral_view_outer" if outer else "lateral_view", *(["posexplode"] if pos else []))
    return Rel(name, name, out, cur.key + ([el] if not pos else [pn]))


def st_sessionize(q: Q, cur: Rel, aggregate: bool | None = None) -> Rel | None:
    rng = q.rng
    tss = [c for c in cur.cols if c.kind == "ts" and not c.nullable] or cur.of("ts")
    parts = [c for c in cur.cols if c.kind in ("fk", "cat") and c.name not in cur.key]
    if not tss or not parts:
        return None
    t, p = rng.choice(tss), rng.choice(parts)
    gap, unit = rng.choice([(30, "MINUTES"), (45, "MINUTES"), (60, "MINUTES"), (2, "HOURS"), (90, "MINUTES")])
    taken = set(cur.names())
    prev = fresh(taken, f"prev_{t.name}")
    flag = fresh(taken, rng.choice(["is_new_session", "new_session_flag", "starts_session"]))
    sess = fresh(taken, rng.choice(["session_no", "session_id", "burst_id"]))
    spec = WSpec([p.name], [f'"{p.name}"'], [t.name] + cur.key, [f'"{t.name}"'] + [f'"{k}"' for k in cur.key],
                 hint=f"w_{clean(p.name)}")
    var = q.win(spec.py(), spec.hint)
    rspec = spec.with_frame(*FRAMES["running"], "running")
    rvar = q.win(rspec.py(), rspec.hint)
    n1, n2, n3 = q.name("with_prev"), q.name("flagged"), q.name("sessionized")
    q.add(n1, f"Previous {t.name} per {p.name}",
          [f"SELECT *, LAG({t.name}) OVER (PARTITION BY {p.name} ORDER BY {', '.join([t.name] + cur.key)}) AS {prev}",
           f"FROM {cur.name}"], f'{n1} = {cur.py}.withColumn("{prev}", F.lag("{t.name}").over({var}))')
    if rng.random() < 0.6:
        cond_sql = f"{prev} IS NULL OR {t.name} > {prev} + INTERVAL {gap} {unit}"
        cond_py = f'F.col("{prev}").isNull() | (F.col("{t.name}") > F.col("{prev}") + F.expr("INTERVAL {gap} {unit}"))'
    else:
        mins = gap if unit == "MINUTES" else gap * 60
        cond_sql = f"{prev} IS NULL OR (UNIX_TIMESTAMP({t.name}) - UNIX_TIMESTAMP({prev})) / 60 > {mins}"
        cond_py = (f'F.col("{prev}").isNull() | ((F.unix_timestamp("{t.name}") - F.unix_timestamp("{prev}")) / 60 > {mins})')
    q.add(n2, f"Flag rows that start a new session (gap > {gap} {unit.lower()})",
          [f"SELECT *, CASE WHEN {cond_sql} THEN 1 ELSE 0 END AS {flag}", f"FROM {n1}"],
          chain(n2, n1, [f'.withColumn("{flag}", F.when({cond_py}, 1).otherwise(0))']))
    q.add(n3, "Running sum of the flag = session number",
          [f"SELECT *, SUM({flag}) OVER ({rspec.sql()}) AS {sess}", f"FROM {n2}"],
          f'{n3} = {n2}.withColumn("{sess}", F.sum("{flag}").over({rvar}))')
    q.feat("sessionize")
    out = list(cur.cols) + [replace(t, name=prev, nullable=True), num_col(flag, "int", 0, 1, False),
                            num_col(sess, "int", 1, 6, False)]
    rel = Rel(n3, n3, out, cur.key)
    if aggregate is False or (aggregate is None and rng.random() < 0.35):
        return rel
    taken = {c.name for c in out}
    mets = [("MIN", t.name, fresh(taken, "session_start")), ("MAX", t.name, fresh(taken, "session_end")),
            ("COUNT", "*", fresh(taken, rng.choice(["events", "n_events", "event_count"])))]
    nums = [c for c in cur.of(*NUM)]
    extra = []
    if nums and rng.random() < 0.6:
        m = rng.choice(nums)
        extra.append((f"SUM({m.name})", f'F.sum("{m.name}")', num_col(fresh(taken, f"session_{m.name}"), m.kind, m.lo, m.hi * 4)))
    dur = fresh(taken, "duration_min")
    name = q.name(rng.choice(["sessions", "session_summary", "per_session"]))
    sql = ["SELECT", f"    {p.name},", f"    {sess},", f"    MIN({t.name}) AS {mets[0][2]},", f"    MAX({t.name}) AS {mets[1][2]},",
           f"    COUNT(*) AS {mets[2][2]},"] + [f"    {e[0]} AS {e[2].name}," for e in extra] + \
          [f"    ROUND((UNIX_TIMESTAMP(MAX({t.name})) - UNIX_TIMESTAMP(MIN({t.name}))) / 60, 1) AS {dur}",
           f"FROM {n3}", f"GROUP BY {p.name}, {sess}"]
    agg = [f'F.min("{t.name}").alias("{mets[0][2]}")', f'F.max("{t.name}").alias("{mets[1][2]}")',
           f'F.count("*").alias("{mets[2][2]}")'] + [f'{e[1]}.alias("{e[2].name}")' for e in extra] + \
          [f'F.round((F.unix_timestamp(F.max("{t.name}")) - F.unix_timestamp(F.min("{t.name}"))) / 60, 1).alias("{dur}")']
    q.add(name, "One row per session", sql, chain(name, n3, [f'.groupBy("{p.name}", "{sess}")', call_py("agg", agg)]))
    q.feat("session_metrics")
    cols = [p, num_col(sess, "int", 1, 6, False), Col(mets[0][2], "ts"), Col(mets[1][2], "ts"),
            num_col(mets[2][2], "int", 1, 6, False)] + [e[2] for e in extra] + [num_col(dur, "double", 0, 240)]
    return Rel(name, name, cols, [p.name, sess])


# ---- interval / range joins (as first stages) -------------------------------
def st_interval(q: Q) -> Rel:
    rng = q.rng
    x = rng.randrange(5)
    name = q.name(rng.choice(["matched", "in_window", "covered", "joined_ranges"]))
    if x == 0:
        c, p = table_rel(TABLE_BY_NAME["claims"]), TABLE_BY_NAME["policies"]
        how = rng.choice(["inner", "left"])
        cols = [c.get("id"), c.get("policy_id"), c.get("claim_date"), c.get("claimed_amount")] + \
            rng.sample([c.get("approved_amount"), c.get("status"), c.get("claim_type"), c.get("adjuster_id")], 2)
        pcols = [replace(p.col("product"), src=("policies", "product"), nullable=True),
                 replace(p.col("premium"), src=("policies", "premium"), nullable=True)]
        items = [bare(x_, "c") for x_ in cols] + [bare(pc, "p") for pc in pcols]
        on_sql = ["p.id = c.policy_id", "c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')"]
        on_py = 'F.col("p.id") == F.col("c.policy_id")) & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))'
        sql = sel_sql(items) + ["FROM claims c", f"{'LEFT ' if how == 'left' else ''}JOIN policies p", f"  ON {on_sql[0]}", f" AND {on_sql[1]}"]
        py = chain(name, 'df_claims.alias("c")', [f'.join(\n        df_policies.alias("p"),\n        (F.col("p.id") == F.col("c.policy_id"))\n        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),\n        "{how}",\n    )', sel_py(items)])
        q.add(name, "Claims that fall inside the policy coverage period", sql, py)
        out = cols + pcols
        key = ["id"]
    elif x == 1:
        how = rng.choice(["inner", "left"])
        r = table_rel(TABLE_BY_NAME["meter_readings"])
        cols = [r.get("id"), r.get("site_id"), r.get("read_at"), r.get("kwh")] + rng.sample([r.get("voltage"), r.get("quality")], 1)
        zone = replace(TABLE_BY_NAME["sites"].col("zone"), src=("sites", "zone"))
        rate = replace(TABLE_BY_NAME["tariff_windows"].col("rate"), src=("tariff_windows", "rate"), nullable=True)
        cost = num_col("energy_cost", "double", 0, 900)
        items = [bare(x_, "r") for x_ in cols] + [bare(zone, "st"), bare(rate, "tw"),
                                                  It("ROUND(r.kwh * tw.rate, 2)", 'F.round(F.col("r.kwh") * F.col("tw.rate"), 2)', cost)]
        sql = sel_sql(items) + ["FROM meter_readings r", "JOIN sites st ON st.id = r.site_id",
                                f"{'LEFT ' if how == 'left' else ''}JOIN tariff_windows tw", "  ON tw.tariff = st.tariff",
                                " AND r.read_at >= tw.valid_from", " AND r.read_at < tw.valid_to"]
        py = chain(name, 'df_meter_readings.alias("r")', [
            '.join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")',
            f'.join(\n        df_tariff_windows.alias("tw"),\n        (F.col("tw.tariff") == F.col("st.tariff"))\n        & (F.col("r.read_at") >= F.col("tw.valid_from"))\n        & (F.col("r.read_at") < F.col("tw.valid_to")),\n        "{how}",\n    )',
            sel_py(items)])
        q.add(name, "Price each reading with the tariff window it falls into", sql, py)
        out = cols + [zone, rate, cost]
        key = ["id"]
    elif x == 2:
        e, t = table_rel(TABLE_BY_NAME["ticket_events"]), table_rel(TABLE_BY_NAME["tickets"])
        cols = [e.get("id"), e.get("ticket_id"), e.get("event_at"), e.get("event_type")]
        tcols = [replace(t.get("priority"), nullable=False), t.get("agent_id")]
        hrs = num_col("hours_since_open", "double", 0, 72)
        n = rng.choice([1, 2, 3, 7])
        items = [bare(x_, "e") for x_ in cols] + [bare(x_, "t") for x_ in tcols] + [
            It("ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2)",
               'F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2)', hrs)]
        sql = sel_sql(items) + ["FROM ticket_events e", "JOIN tickets t", "  ON t.id = e.ticket_id",
                                " AND e.event_at >= t.opened_at",
                                f" AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL {n} DAYS)"]
        py = chain(name, 'df_ticket_events.alias("e")', [
            f'.join(\n        df_tickets.alias("t"),\n        (F.col("t.id") == F.col("e.ticket_id"))\n        & (F.col("e.event_at") >= F.col("t.opened_at"))\n        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL {n} DAYS"))),\n        "inner",\n    )',
            sel_py(items)])
        q.add(name, "Events that happened while the ticket was open", sql, py)
        out = cols + tcols + [hrs]
        key = ["id"]
    else:
        # self-join over time: prior rows of the same key within a look-back window
        tbl, k, tcol = rng.choice([("plays", "listener_id", "played_at"), ("claims", "policy_id", "claim_date"),
                                   ("stock_moves", "sku_id", "moved_at"), ("meter_readings", "site_id", "read_at"),
                                   ("enrollments", "student_id", "enrolled_at"), ("tickets", "agent_id", "opened_at")])
        T = table_rel(TABLE_BY_NAME[tbl])
        tc = T.get(tcol)
        if tc.kind == "date":
            n = rng.choice([30, 60, 90, 180])
            win_sql = f"b.{tcol} >= DATE_SUB(a.{tcol}, {n})"
            win_py = f'(F.col("b.{tcol}") >= F.date_sub(F.col("a.{tcol}"), {n}))'
            label = f"{n}d"
        else:
            n, unit = rng.choice([(1, "DAYS"), (7, "DAYS"), (12, "HOURS"), (3, "DAYS")])
            win_sql = f"b.{tcol} >= a.{tcol} - INTERVAL {n} {unit}"
            win_py = f'(F.col("b.{tcol}") >= F.col("a.{tcol}") - F.expr("INTERVAL {n} {unit}"))'
            label = f"{n}{unit[0].lower()}"
        nums = [c for c in T.cols if c.kind in NUM]
        m = rng.choice(nums) if nums else None
        cnt = num_col(f"prior_rows_{label}", "int", 0, 8, False)
        tot = num_col(f"prior_{m.name}_{label}", m.kind, 0, m.hi * 5) if m else None
        gcols = [T.get("id"), T.get(k), tc]
        sql = ["SELECT", "    a.id,", f"    a.{k},", f"    a.{tcol},", f"    COUNT(b.id) AS {cnt.name}" + ("," if m else "")]
        if m:
            sql.append(f"    SUM(b.{m.name}) AS {tot.name}")
        sql += [f"FROM {tbl} a", f"LEFT JOIN {tbl} b", f"  ON b.{k} = a.{k}", f" AND b.{tcol} < a.{tcol}", f" AND {win_sql}",
                f"GROUP BY a.id, a.{k}, a.{tcol}"]
        agg = [f'F.count("b.id").alias("{cnt.name}")'] + ([f'F.sum("b.{m.name}").alias("{tot.name}")'] if m else [])
        py = chain(name, f'df_{tbl}.alias("a")', [
            f'.join(\n        df_{tbl}.alias("b"),\n        (F.col("b.{k}") == F.col("a.{k}"))\n        & (F.col("b.{tcol}") < F.col("a.{tcol}"))\n        & {win_py},\n        "left",\n    )',
            f'.groupBy("a.id", "a.{k}", "a.{tcol}")', call_py("agg", agg)])
        q.add(name, "Count earlier rows of the same key inside the look-back window", sql, py)
        out = gcols + [cnt] + ([tot] if m else [])
        key = ["id"]
        q.feat("self_join_range")
        return Rel(name, name, out, key)
    q.feat("range_join")
    return Rel(name, name, out, key)


# ---------------------------------------------------------------------------
# finals
# ---------------------------------------------------------------------------
def fin_select(q: Q, cur: Rel):
    rng = q.rng
    keep = [c for c in cur.cols if rng.random() < 0.75 or c.name in cur.key[:1]]
    taken = set(cur.names())
    derived = [d for d in (derive(rng, cur.cols, taken) for _ in range(rng.randint(0, 2))) if d]
    items = [bare(c) for c in keep] + derived
    preds = preds_for(rng, cur.cols, 1) if rng.random() < 0.3 else []
    distinct = False
    if rng.random() < 0.08 and not derived:
        items = [bare(c) for c in rng.sample(keep, min(len(keep), rng.randint(1, 3)))]
        distinct = True
    orderable = [i for i in items if i.col.kind in ("int", "double", "date", "ts", "cat", "str", "fk", "id")]
    order, order_py, ocols = [], [], []
    if orderable and rng.random() < 0.85:
        for i in rng.sample(orderable, min(len(orderable), rng.randint(1, 2))):
            desc = rng.random() < 0.5
            nl = rng.random() < 0.25
            s = f"{i.col.name}{' DESC' if desc else ''}{(' NULLS FIRST' if desc else ' NULLS LAST') if nl else ''}"
            if nl:
                p = f'F.col("{i.col.name}").{"desc_nulls_first" if desc else "asc_nulls_last"}()'
            else:
                p = f'F.col("{i.col.name}").desc()' if desc else f'F.col("{i.col.name}").asc()' if rng.random() < 0.3 else f'"{i.col.name}"'
            order.append(s)
            order_py.append(p)
            ocols.append(i.col.name)
    limit = None
    out_names = {i.col.name for i in items}
    if order and not distinct and rng.random() < 0.25 and all(k in out_names for k in cur.key):
        for k in cur.key:
            if k not in ocols:
                order.append(k)
                order_py.append(f'"{k}"')
                ocols.append(k)
        limit = rng.choice([5, 10, 20])
    sql = sel_sql(items, distinct) + [f"FROM {cur.name}"] + where_lines(preds) + \
        ([f"ORDER BY {', '.join(order)}"] if order else []) + ([f"LIMIT {limit}"] if limit else [])
    steps = ([f".filter({py_and(preds)})"] if preds else []) + [sel_py(items)] + ([".distinct()"] if distinct else []) + \
        ([call_py("orderBy", order_py)] if order else []) + ([f".limit({limit})"] if limit else [])
    q.order_cols = ocols
    if limit:
        q.feat("order_limit")
    return sql, chain("result", cur.py, steps), "Final projection"


def fin_grouping(q: Q, cur: Rel):
    rng = q.rng
    keys = [c for c in cur.cols if c.kind in ("cat", "fk", "bool") or (c.kind == "int" and c.hi - c.lo <= 12)]
    if len(keys) < 2:
        return None
    ks = rng.sample(keys, min(len(keys), rng.randint(2, 3)))
    taken = set(cur.names())
    mets = agg_metrics(rng, cur, taken, rng.randint(1, 3))
    mode = rng.choice(["ROLLUP", "CUBE", "SETS", "SETS"])
    names = [k.name for k in ks]
    if mode == "SETS":
        sets = [names, names[:1]] + ([names[1:2]] if rng.random() < 0.5 else []) + ([[]] if rng.random() < 0.6 else [])
        gb = "GROUP BY GROUPING SETS (" + ", ".join("(" + ", ".join(s) + ")" for s in sets) + ")"
        gpy = ".groupingSets(\n        [" + ", ".join("[" + ", ".join(f'"{n}"' for n in s) + "]" for s in sets) + \
              "],\n        " + ", ".join(f'"{n}"' for n in names) + ",\n    )"
    else:
        gb = f"GROUP BY {mode}({', '.join(names)})"
        gpy = f".{mode.lower()}({', '.join(chr(34) + n + chr(34) for n in names)})"
    label = rng.random() < 0.4
    gid = rng.random() < 0.45
    gid_item = None
    if gid:
        if rng.random() < 0.5:
            gid_item = ("GROUPING_ID()", "F.grouping_id()", num_col(fresh(taken, rng.choice(["gid", "grouping_level"])), "int", 0, 7, False))
        else:
            k0 = ks[0].name
            gid_item = (f"GROUPING({k0})", f'F.grouping("{k0}")', num_col(fresh(taken, f"is_total_{k0}"), "int", 0, 1, False))
    sel_s, sel_p = [], []
    for k in ks:
        if label and k.kind in ("cat",):
            sel_s.append(f"COALESCE({k.name}, 'ALL') AS {k.name}")
            sel_p.append(f'F.coalesce(F.col("{k.name}"), F.lit("ALL")).alias("{k.name}")')
        else:
            sel_s.append(k.name)
            sel_p.append(f'"{k.name}"')
    extra = [gid_item] if gid_item else []
    sel_s += [f"{m[0]} AS {m[2].name}" for m in extra + mets]
    sel_p += [f'"{m[2].name}"' for m in extra + mets]
    order = rng.random() < 0.7
    osql = [f"ORDER BY {', '.join(k + ' NULLS LAST' for k in names)}"] if order else []
    opy = [call_py("orderBy", [f'F.col("{k}").asc_nulls_last()' for k in names])] if order else []
    sql = ["SELECT"] + [f"    {s}," for s in sel_s[:-1]] + [f"    {sel_s[-1]}", f"FROM {cur.name}", gb] + osql
    steps = [gpy, call_py("agg", [f'{m[1]}.alias("{m[2].name}")' for m in extra + mets])]
    if label and any(k.kind == "cat" for k in ks):
        steps.append(call_py("select", sel_p))
    steps += opy
    q.order_cols = names if order else []
    q.feat({"ROLLUP": "rollup", "CUBE": "cube", "SETS": "grouping_sets"}[mode], *(["grouping_id"] if gid else []))
    return sql, chain("result", cur.py, steps), "Subtotals with " + {"ROLLUP": "ROLLUP", "CUBE": "CUBE", "SETS": "GROUPING SETS"}[mode]


SAFE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def fin_pivot(q: Q, cur: Rel):
    rng = q.rng
    pcs = [c for c in cur.cols if c.kind == "cat" and len([v for v in (src_vals(c) or c.pool) if isinstance(v, str) and SAFE.match(v)]) >= 2]
    nums = cur.of(*NUM)
    if not pcs or not nums:
        return None
    pc = rng.choice(pcs)
    keys = [c for c in cur.cols if c.name != pc.name and (c.kind in ("cat", "fk", "bool", "ts", "date") or
                                                           (c.kind == "int" and c.hi - c.lo <= 12))]
    if not keys:
        return None
    ks = rng.sample(keys, rng.randint(1, min(2, len(keys))))
    vcands = [n for n in nums if n.name not in {k.name for k in ks}]
    if not vcands:
        return None
    v = rng.choice(vcands)
    pool = [x for x in (src_vals(pc) or pc.pool) if isinstance(x, str) and SAFE.match(x)]
    vals = rng.sample(pool, rng.randint(2, min(4, len(pool))))
    two = rng.random() < 0.25
    fn = rng.choice(["SUM", "AVG", "MAX", "COUNT", "SUM", "MIN"])
    if two:
        fn2 = rng.choice([f for f in ("SUM", "COUNT", "MAX") if f != fn])
        a1, a2 = rng.choice(["total", "amount"]) if fn == "SUM" else fn.lower() + "_v", "n" if fn2 == "COUNT" else fn2.lower() + "_v"
        agg_sql = f"{fn}({v.name}) AS {a1}, {fn2}({v.name}) AS {a2}"
        agg_py = f'.agg(F.{fn.lower()}("{v.name}").alias("{a1}"), F.{fn2.lower()}("{v.name}").alias("{a2}"))'
    else:
        agg_sql = f"{fn}({v.name})"
        agg_py = f'.agg(F.{fn.lower()}("{v.name}"))'
    kn = [k.name for k in ks]
    sql = ["SELECT *", "FROM (", f"    SELECT {', '.join(kn + [pc.name, v.name])}", f"    FROM {cur.name}", ") src",
           "PIVOT (", f"    {agg_sql}", f"    FOR {pc.name} IN ({', '.join(map(sql_lit, vals))})", ")"]
    order = rng.random() < 0.6
    if order:
        sql.append(f"ORDER BY {', '.join(kn)}")
    steps = [f'.groupBy({", ".join(chr(34) + k + chr(34) for k in kn)})',
             f'.pivot("{pc.name}", [{", ".join(map(py_lit, vals))}])', agg_py] + \
            ([call_py("orderBy", [f'"{k}"' for k in kn])] if order else [])
    q.order_cols = kn if order else []
    q.feat("pivot", *(["pivot_multi_agg"] if two else []))
    return sql, chain("result", cur.py, steps), "Native SQL PIVOT (long to wide)"


def fin_unpivot(q: Q, cur: Rel):
    rng = q.rng
    ids = ([c for c in cur.cols if c.name in cur.key] or [c for c in cur.cols if c.kind in ("cat", "fk")][:1])[:2]
    if not ids:
        return None
    nums = [c for c in cur.of(*NUM) if c.name not in {i.name for i in ids}]
    by_kind = {}
    for n in nums:
        by_kind.setdefault(n.kind, []).append(n)
    groups = [g for g in by_kind.values() if len(g) >= 2]
    if not groups:
        return None
    g = rng.choice(groups)
    vs = rng.sample(g, rng.randint(2, min(4, len(g))))
    var = rng.choice(["metric", "metric_name", "measure", "field"])
    val = rng.choice(["value", "metric_value", "reading", "amount"])
    include = rng.random() < 0.2
    aliased = rng.random() < 0.3
    lbl = {c.name: fresh(set(cur.names()) | {var, val}, rng.choice([f"{c.name}_v", f"m_{c.name}"])) for c in vs} if aliased else {}
    in_list = ", ".join(f"{c.name} AS {lbl[c.name]}" if aliased else c.name for c in vs)
    idn = [c.name for c in ids]
    sql = [f"SELECT {', '.join(idn + [var, val])}", "FROM (", f"    SELECT {', '.join(idn + [c.name for c in vs])}",
           f"    FROM {cur.name}", ") src", f"UNPIVOT{' INCLUDE NULLS' if include else ''} (",
           f"    {val} FOR {var} IN ({in_list})", ") unp"]
    order = rng.random() < 0.4
    if order:
        sql.append(f"ORDER BY {', '.join(idn + [var])}")
    sel = [f'"{n}"' for n in idn] + ([f'F.col("{c.name}").alias("{lbl[c.name]}")' for c in vs] if aliased else [f'"{c.name}"' for c in vs])
    values = [lbl[c.name] if aliased else c.name for c in vs]
    steps = [call_py("select", sel),
             ".unpivot(\n        ids=[" + ", ".join(f'"{n}"' for n in idn) + "],\n        values=[" +
             ", ".join(f'"{n}"' for n in values) + f'],\n        variableColumnName="{var}",\n        valueColumnName="{val}",\n    )']
    if not include:
        steps.append(f'.filter(F.col("{val}").isNotNull())')
    if order:
        steps.append(call_py("orderBy", [f'"{n}"' for n in idn + [var]]))
    q.order_cols = idn + [var] if order else []
    q.feat("unpivot", *(["unpivot_include_nulls"] if include else []), *(["unpivot_alias"] if aliased else []))
    return sql, chain("result", cur.py, steps), "Native SQL UNPIVOT (wide to long)"


# ---------------------------------------------------------------------------
# set operations
# ---------------------------------------------------------------------------
KGROUP = {"id": "int", "fk": "int", "int": "int", "double": "double", "cat": "str", "str": "str", "date": "date",
          "ts": "ts", "bool": "bool"}
GENERIC = {"int": ["entity_id", "ref_id", "key_id", "qty"], "double": ["amount", "metric", "value"],
           "str": ["label", "category", "name"], "date": ["event_date", "day"], "ts": ["event_time", "happened_at"],
           "bool": ["flag"]}


def pick_shape_cols(rng, rel: Rel, shape: list) -> list | None:
    used, out = set(), []
    for g in shape:
        cands = [c for c in rel.cols if KGROUP.get(c.kind) == g and c.name not in used]
        if not cands:
            return None
        c = rng.choice(cands)
        used.add(c.name)
        out.append(c)
    return out


def fin_setop(q: Q, rng: random.Random):
    domain_tables = {}
    for t in TABLES:
        domain_tables.setdefault(t.domain, []).append(t)
    tables = rng.choice(list(domain_tables.values()))
    op = rng.choices(["UNION ALL", "UNION", "INTERSECT", "EXCEPT", "INTERSECT ALL", "EXCEPT ALL", "UNION3"],
                     [36, 18, 9, 12, 3, 5, 17])[0]
    n_br = 3 if op == "UNION3" else 2
    op_sql = "UNION ALL" if op == "UNION3" else op
    if op.startswith(("INTERSECT", "EXCEPT")):
        shape = rng.choice([["int"], ["int"], ["str"], ["int", "str"]])
    else:
        shape = [rng.choice(["int", "int", "str"])] + rng.sample(["double", "ts", "date", "str", "int"], rng.randint(1, 2))
    rels = []
    for i in range(n_br):
        for _ in range(12):
            t = rng.choice(tables)
            rel = table_rel(t)
            cols = pick_shape_cols(rng, rel, shape)
            if cols:
                rels.append((t, rel, cols))
                break
        else:
            return None
    # optionally route the first branch through a CTE pipeline stage
    pre = None
    if rng.random() < 0.3:
        t0 = rels[0][0]
        pre = st_source(q, t0, n_derive=(0, 1))
        cols = pick_shape_cols(rng, pre, shape)
        if cols:
            rels[0] = (t0, pre, cols)
    same_names = rng.random() < 0.45
    tag = rng.random() < 0.45 and op_sql.startswith("UNION")
    tag_name = rng.choice(["source", "src", "record_type", "origin"])
    tag_first = rng.random() < 0.4
    first_names = []
    for c, g in zip(rels[0][2], shape):
        first_names.append(c.name if rng.random() < 0.5 else fresh(set(first_names), rng.choice(GENERIC[g])))
    branches_sql, branches_py, vars_ = [], [], []
    positional = False
    for bi, (t, rel, cols) in enumerate(rels):
        items = []
        for j, (c, want) in enumerate(zip(cols, first_names)):
            if bi == 0 or same_names:
                items.append(renamed(c, want))
            else:
                items.append(bare(c))
                if c.name != want:
                    positional = True
        if tag:
            lab = SINGULAR[t.name] if bi == 0 or rels[bi][0].name != rels[0][0].name else f"{SINGULAR[t.name]}_{bi + 1}"
            ti = It(sql_lit(lab), f"F.lit({py_lit(lab)})", Col(tag_name, "str"))
            if bi > 0 and not same_names and rng.random() < 0.6:
                ti = It(sql_lit(lab), f"F.lit({py_lit(lab)})", Col(tag_name, "str"))
            items = [ti] + items if tag_first else items + [ti]
        preds = preds_for(rng, rel.cols, rng.randint(0, 2), allow_or=False) if rng.random() < 0.8 else []
        src_sql = rel.name
        branches_sql.append(sel_sql(items) + [f"FROM {src_sql}"] + where_lines(preds))
        var = q.name(f"{SINGULAR[t.name]}_side" if rng.random() < 0.6 else f"{SINGULAR[t.name]}_rows")
        vars_.append(var)
        branches_py.append(chain(var, rel.py, ([f".filter({py_and(preds)})"] if preds else []) + [sel_py(items)]))
    for b in branches_py:
        q.blocks.append(b)
    # PySpark method
    if op_sql in ("UNION ALL", "UNION"):
        meth = "unionByName" if (same_names or n_br == 1) and not positional else "union"
        if same_names and rng.random() < 0.3:
            meth = "union"
        py_ops = [f".{meth}({v})" for v in vars_[1:]] + ([".distinct()"] if op_sql == "UNION" else [])
        q.feat("union_all" if op_sql == "UNION ALL" else "union_distinct", "union_positional" if positional else "union_by_name",
               *(["union_3way"] if n_br == 3 else []), *(["union_literal_tag"] if tag else []))
    else:
        meth = {"INTERSECT": "intersect", "EXCEPT": "subtract", "INTERSECT ALL": "intersectAll", "EXCEPT ALL": "exceptAll"}[op_sql]
        py_ops = [f".{meth}({vars_[1]})"]
        q.feat(op_sql.lower().replace(" ", "_"))
        if op_sql == "EXCEPT" and rng.random() < 0.15:
            op_sql = "MINUS"
    body = []
    for i, b in enumerate(branches_sql):
        if i:
            body.append(op_sql)
        body += b
    out_names = ([tag_name] if tag and tag_first else []) + first_names + ([tag_name] if tag and not tag_first else [])
    x = rng.random()
    if x < 0.3 and op_sql.startswith("UNION"):
        # union then aggregate
        cname = q.name(rng.choice(["combined", "all_events", "unioned", "stacked"]))
        q.ctes.append((cname, "Stack the branches", body))
        py_body = chain(cname, vars_[0], py_ops)
        q.blocks.append(py_body)
        gkeys = [tag_name] if tag else [first_names[shape.index("str")]] if "str" in shape else [first_names[0]]
        num_names = [n for n, g in zip(first_names, shape) if g in ("double", "int") and n not in gkeys]
        mets = [("COUNT(*)", 'F.count("*")', "n_rows")]
        if num_names:
            nm = rng.choice(num_names)
            mets.append((f"SUM({nm})", f'F.sum("{nm}")', f"total_{nm}"))
        ts_names = [n for n, g in zip(first_names, shape) if g in ("ts", "date")]
        if ts_names:
            mets.append((f"MAX({ts_names[0]})", f'F.max("{ts_names[0]}")', f"latest_{ts_names[0]}"))
        sql = [f"SELECT {gkeys[0]}, " + ", ".join(f"{m[0]} AS {m[2]}" for m in mets), f"FROM {cname}",
               f"GROUP BY {gkeys[0]}", f"ORDER BY {gkeys[0]}"]
        py = chain("result", cname, [f'.groupBy("{gkeys[0]}")', call_py("agg", [f'{m[1]}.alias("{m[2]}")' for m in mets]),
                                     f'.orderBy("{gkeys[0]}")'])
        q.order_cols = [gkeys[0]]
        q.feat("union_then_aggregate")
        return sql, py, None
    sql = list(body)
    steps = list(py_ops)
    if x < 0.7:
        on = rng.sample(out_names, rng.randint(1, min(2, len(out_names))))
        desc = rng.random() < 0.4
        sql.append("ORDER BY " + ", ".join(f"{n}{' DESC' if desc and i == 0 else ''}" for i, n in enumerate(on)))
        steps.append(call_py("orderBy", [f'F.col("{n}").desc()' if desc and i == 0 else f'"{n}"' for i, n in enumerate(on)]))
        q.order_cols = on
    return sql, chain("result", vars_[0], steps), None


# ---------------------------------------------------------------------------
# two-source FULL OUTER JOIN
# ---------------------------------------------------------------------------
def fin_full_outer(q: Q, rng: random.Random):
    splits = [("stock_moves", "move_type", ["INBOUND", "OUTBOUND", "TRANSFER"], "qty", ["warehouse_id", "sku_id"]),
              ("claims", "status", ["APPROVED", "REJECTED", "PAID", "OPEN"], "claimed_amount", ["adjuster_id", "policy_id"]),
              ("plays", "device", ["phone", "desktop", "car", "speaker"], "ms_played", ["track_id", "listener_id"]),
              ("enrollments", "grade_status", ["PASS", "FAIL", "WITHDRAWN"], "score", ["course_id", "student_id"]),
              ("ticket_events", "event_type", ["replied", "escalated", "resolved", "reopened"], None, ["ticket_id"]),
              ("meter_readings", "quality", ["OK", "EST", "BAD"], "kwh", ["site_id"])]
    tbl, cat, vals, m, keys = rng.choice(splits)
    T = TABLE_BY_NAME[tbl]
    tcol = next(c for c in T.columns if c.kind in ("ts", "date"))
    by_month = rng.random() < 0.4
    k_name = f"{tcol.name}_month" if by_month else rng.choice(keys)
    v1, v2 = rng.sample(vals, 2)
    sides = []
    for v in (v1, v2):
        nm = q.name(f"{clean(v)}_{'monthly' if by_month else 'by_' + clean(k_name.replace('_id', ''))}")
        mets = [("COUNT(*)", 'F.count("*")', f"{clean(v)}_count")]
        if m:
            if rng.random() < 0.6:
                mets.append((f"SUM({m})", f'F.sum("{m}")', f"{clean(v)}_total_{m}"))
            else:
                mets.append((f"ROUND(AVG({m}), 2)", f'F.round(F.avg("{m}"), 2)', f"{clean(v)}_avg_{m}"))
        if by_month:
            key_sql, key_py = f"DATE_TRUNC('MONTH', {tcol.name}) AS {k_name}", f'F.date_trunc("month", "{tcol.name}").alias("{k_name}")'
            gb = f"GROUP BY DATE_TRUNC('MONTH', {tcol.name})"
        else:
            key_sql, key_py, gb = k_name, f'"{k_name}"', f"GROUP BY {k_name}"
        sql = [f"SELECT {key_sql}, " + ", ".join(f"{s} AS {n}" for s, _, n in mets), f"FROM {tbl}",
               f"WHERE {cat} = {sql_lit(v)}", gb]
        py = chain(nm, f"df_{tbl}", [f'.filter(F.col("{cat}") == {py_lit(v)})', f".groupBy({key_py})",
                                     call_py("agg", [f'{p}.alias("{n}")' for _, p, n in mets])])
        q.add(nm, f"{v} rows per {k_name}", sql, py)
        sides.append((nm, mets))
    (a, am), (b, bm) = sides
    items_s = [f"COALESCE(a.{k_name}, b.{k_name}) AS {k_name}"]
    items_p = [f'F.coalesce(F.col("a.{k_name}"), F.col("b.{k_name}")).alias("{k_name}")']
    for side, mets in (("a", am), ("b", bm)):
        for s, _, n in mets:
            if s == "COUNT(*)" or rng.random() < 0.5:
                items_s.append(f"COALESCE({side}.{n}, 0) AS {n}")
                items_p.append(f'F.coalesce(F.col("{side}.{n}"), F.lit(0)).alias("{n}")')
            else:
                items_s.append(f"{side}.{n}")
                items_p.append(f'F.col("{side}.{n}")')
    c1, c2 = am[0][2], bm[0][2]
    diff = f"{c1}_minus_{c2}" if rng.random() < 0.5 else "net_count"
    items_s.append(f"COALESCE(a.{c1}, 0) - COALESCE(b.{c2}, 0) AS {diff}")
    items_p.append(f'(F.coalesce(F.col("a.{c1}"), F.lit(0)) - F.coalesce(F.col("b.{c2}"), F.lit(0))).alias("{diff}")')
    sql = ["SELECT"] + [f"    {s}," for s in items_s[:-1]] + [f"    {items_s[-1]}", f"FROM {a} a", f"FULL OUTER JOIN {b} b",
                                                                 f"  ON a.{k_name} = b.{k_name}", f"ORDER BY {k_name}"]
    py = chain("result", f'{a}.alias("a")', [f'.join({b}.alias("b"), F.col("a.{k_name}") == F.col("b.{k_name}"), "full")',
                                              call_py("select", items_p), f'.orderBy("{k_name}")'])
    q.order_cols = [k_name]
    q.feat("full_outer_join", "coalesce_keys")
    return sql, py, "Combine both sides; keys missing on one side get zeros"


# ---------------------------------------------------------------------------
# families
# ---------------------------------------------------------------------------
FACT_TABLES = ["plays", "claims", "stock_moves", "meter_readings", "enrollments", "tickets", "ticket_events", "policies",
               "tracks"]
ALL_TABLES = [t.name for t in TABLES]


def pick_table(rng, facts=0.7) -> Table:
    return TABLE_BY_NAME[rng.choice(FACT_TABLES) if rng.random() < facts else rng.choice(ALL_TABLES)]


MIDDLE = {"derive": 14, "join": 18, "windows": 16, "exists": 12, "child_stats": 9, "join_child": 6, "dedup": 5,
          "filter_rank": 6, "compare": 5, "explode": 4, "aggregate": 8}


def run_stage(q: Q, kind: str, cur: Rel) -> Rel | None:
    fn = {"derive": st_derive, "join": st_join, "windows": st_windows, "exists": st_exists, "child_stats": st_child_stats,
          "join_child": st_join_child, "dedup": st_dedup, "filter_rank": st_filter_rank, "compare": st_compare_group,
          "explode": st_explode, "aggregate": st_aggregate}[kind]
    return fn(q, cur)


def middle(q: Q, cur: Rel, n: int, allow_agg: bool = True) -> Rel:
    rng = q.rng
    done, aggs, tries = 0, 0, 0
    while done < n and tries < n * 6:
        tries += 1
        kinds = [k for k in MIDDLE if (allow_agg or k != "aggregate") and not (k == "aggregate" and aggs >= 1)]
        k = rng.choices(kinds, [MIDDLE[x] for x in kinds])[0]
        if k == "filter_rank" and not cur.rank_cols:
            continue
        nxt = run_stage(q, k, cur)
        if nxt is not None:
            cur = nxt
            done += 1
            aggs += k == "aggregate"
    return cur


def final(q: Q, cur: Rel, choice: str | None = None):
    rng = q.rng
    order = [choice] if choice else []
    order += rng.choices(["select", "grouping", "pivot", "unpivot"], [70, 10, 10, 10], k=1) + ["select"]
    for f in order:
        res = {"select": fin_select, "grouping": fin_grouping, "pivot": fin_pivot, "unpivot": fin_unpivot}[f](q, cur)
        if res:
            return res
    return fin_select(q, cur)


def build(rng: random.Random, family: str):
    q = Q(rng)
    if family == "pipeline":
        cur = st_source(q, pick_table(rng))
        cur = middle(q, cur, rng.randint(2, 6))
        res = final(q, cur)
    elif family == "window_heavy":
        cur = st_source(q, pick_table(rng, 0.85), n_derive=(0, 2))
        if rng.random() < 0.5:
            cur = st_join(q, cur) or cur
        cur = st_windows(q, cur, n=(8, 16), specs=(3, 6)) or cur
        if rng.random() < 0.5:
            cur = (st_filter_rank(q, cur) if cur.rank_cols and rng.random() < 0.6 else st_derive(q, cur)) or cur
        if rng.random() < 0.55:
            cur = st_windows(q, cur, n=(3, 8), specs=(2, 4)) or cur
        if rng.random() < 0.25:
            cur = st_aggregate(q, cur) or cur
            if rng.random() < 0.5:
                cur = st_windows(q, cur, n=(2, 4), specs=(1, 2)) or cur
        res = final(q, cur, "select")
    elif family == "setop":
        res = fin_setop(q, rng)
    elif family == "sessionize":
        t = TABLE_BY_NAME[rng.choice(["plays", "stock_moves", "meter_readings", "ticket_events", "enrollments", "tickets"])]
        cur = st_source(q, t, n_derive=(0, 1), n_pred=(0, 1))
        cur = st_sessionize(q, cur) or cur
        cur = middle(q, cur, rng.randint(0, 2))
        res = final(q, cur)
    elif family == "interval":
        cur = st_interval(q)
        cur = middle(q, cur, rng.randint(1, 3))
        res = final(q, cur)
    elif family == "full_outer":
        res = fin_full_outer(q, rng)
    elif family == "grouping":
        cur = st_source(q, pick_table(rng))
        cur = middle(q, cur, rng.randint(1, 3), allow_agg=False)
        res = final(q, cur, "grouping")
    elif family == "reshape":
        cur = st_source(q, pick_table(rng))
        cur = middle(q, cur, rng.randint(1, 3))
        res = final(q, cur, rng.choice(["pivot", "unpivot"]))
    else:
        raise ValueError(family)
    if res is None:
        return None
    sql_lines, py, comment = res
    sql, code = q.render(sql_lines, py, comment)
    return sql, code, q.features, q.order_cols


FAMILIES = {"pipeline": 30, "window_heavy": 16, "setop": 15, "sessionize": 8, "interval": 7, "full_outer": 5,
            "grouping": 10, "reshape": 9}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=11000)
    ap.add_argument("--seed", type=int, default=8)
    ap.add_argument("--out", default="raw_pairs")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("cx*.sql"):
        f.unlink()
    for f in out.glob("cx*.py"):
        f.unlink()
    rng = random.Random(args.seed)
    fams = list(FAMILIES)
    seen, metas, fails = set(), [], 0
    i = 0
    while len(metas) < args.n:
        fam = rng.choices(fams, [FAMILIES[f] for f in fams])[0]
        sub = random.Random(rng.random())
        try:
            res = build(sub, fam)
        except Exception as e:  # noqa: BLE001  (generator bug -> skip, counted)
            fails += 1
            if fails < 20:
                print(f"generator error in {fam}: {type(e).__name__}: {e}")
            continue
        if not res:
            continue
        sql, code, feats, order_cols = res
        norm = re.sub(r"\s+", " ", sql.lower())
        if norm in seen:
            continue
        seen.add(norm)
        i += 1
        pid = f"cx{i:05d}"
        (out / f"{pid}.sql").write_text(sql + "\n", encoding="utf-8")
        (out / f"{pid}.py").write_text(code, encoding="utf-8")
        metas.append({"id": pid, "family": f"cx_{fam}", "features": sorted(set(feats)), "order_cols": order_cols})
    with open(out / "meta_complex.jsonl", "w", encoding="utf-8") as f:
        for m in metas:
            f.write(json.dumps(m) + "\n")
    from collections import Counter
    print(json.dumps({"written": len(metas), "generator_errors": fails,
                      "families": dict(Counter(m["family"] for m in metas))}, indent=2))


if __name__ == "__main__":
    main()
