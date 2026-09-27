"""Compositional SQL -> PySpark pair generator (dataset v2).

Each sample picks a query *family* (plain select, aggregation, joins, windows,
top-N, CTE chains, subqueries, set operations, pivot/explode, ...) and fills it
with random building blocks drawn from the table catalog in schemas.py.  The
SQL and PySpark texts are rendered from the same objects (see sqlspec.py), so
they agree by construction; verify_pairs.py then proves it by executing both.

Output: <out>/genNNNNN.sql + genNNNNN.py, plus <out>/meta_generated.jsonl with
each pair's feature tags (used for the leakage-free split) and ORDER BY columns
(used by the execution check).

Usage:  python generate_dataset.py --n 15000 --out raw_pairs
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterable, TypeVar

import advanced
from schemas import TABLE_BY_NAME, Column, Table, train_tables
from sqlspec import (Agg, And, Between, BinOp, BoolTest, Case, Cast, Cmp, Col, Ctx, E, Fn, FnPred, InList, IsNull,
                     Join, Like, Lit, NoArgFn, Not, Or, PyVar, Raw, Rel, Sel, Semi, Style, WinFn, WinSpec, call,
                     chain, find, out_columns, pyq, render_code)

rng = random.Random(0)
TABLES = train_tables()
TRAIN_NAMES = {t.name for t in TABLES}


class Skip(Exception):
    """The chosen table can't support this family; try again."""


class Star(E):
    kind = "int"

    def sql(self, st):
        return "*"

    def py(self, ctx):
        return '"*"'


T = TypeVar("T")

# a lazily-built candidate: () -> (expression, output name[, feature tag])
DeriveOpt = Callable[[], "tuple[E, str, str]"]
AggOpt = Callable[[], "tuple[E, str]"]


def choice(xs: Iterable[T]) -> T:
    items = list(xs)
    if not items:
        raise Skip
    return rng.choice(items)


def chance(p):
    return rng.random() < p


def wchoice(pairs):
    items, weights = zip(*pairs)
    return rng.choices(items, weights)[0]


def singular(name):
    if name.endswith("ies"):
        return name[:-3] + "y"
    if name.endswith("s"):
        return name[:-1]
    return name


def ident(v):
    return re.sub(r"[^a-z0-9]+", "_", str(v).lower()).strip("_")


def snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def base_rel(t, alias=None):
    return Rel(t.name, f"df_{t.name}", list(t.columns), alias=alias, table=t)


def cols(rel, *kinds, nonnull=False):
    return [c for c in rel.cols if c.kind in kinds and (c.nullable is False or not nonnull)]


def fks(t):
    return [c for c in t.columns if c.kind == "fk" and c.ref in TRAIN_NAMES and c.ref != t.name]


def children(t):
    return [(ct, c) for ct in TABLES for c in fks(ct) if c.ref == t.name]


def make_alias(name, used):
    a = "".join(w[0] for w in name.split("_"))
    if a in used:
        a = name[:2] if len(name.split("_")) == 1 else name.split("_")[0][:1] + name.split("_")[1][:2]
    base, i = a, 2
    while a in used:
        a, i = f"{base}{i}", i + 1
    used.add(a)
    return a


def aliases_for(st, names):
    used = set()
    if st.short_alias or len(set(names)) < len(names):
        return [make_alias(n, used) for n in names]
    return list(names)


# ---------------------------------------------------------------------------
# Literals
# ---------------------------------------------------------------------------
def nice(lo, hi, kind="double"):
    span = hi - lo
    v = rng.uniform(lo + 0.2 * span, hi - 0.2 * span)
    if kind in ("int", "fk", "id"):
        return int(round(v))
    for step in (1000, 100, 10, 1):
        if span >= step * 20:
            return int(round(v / step) * step)
    if span >= 2:
        return round(v, 1)
    return round(v, 2)


def pool_val(c):
    return choice(v for v in c.pool if "'" not in v)


def rand_date():
    d = date(2023, 1, 1) + timedelta(days=rng.randint(60, 3 * 365 - 60))
    if chance(0.6):
        d = d.replace(day=1)
    return d.isoformat()


def rand_date_pair():
    a, b = sorted({rand_date(), rand_date()})
    if a == b:
        raise Skip
    return a, b


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------
FILTERABLE = ("int", "double", "fk", "cat", "str", "date", "ts", "bool", "array")


def atom(rel, col=None):
    col = col or choice(c for c in rel.cols if c.kind in FILTERABLE)
    x, k, r = rel.ref(col), col.kind, rng.random()
    if k in ("int", "double"):
        if col.nullable and r < 0.08:
            return IsNull(x, neg=chance(0.7)), "null"
        if r < 0.78:
            ops = [">", ">=", "<", "<="] + (["=", "<>"] if k == "int" else [])
            return Cmp(x, choice(ops), Lit(nice(col.lo, col.hi, k))), "cmp"
        lo, hi = sorted({nice(col.lo, col.hi, k), nice(col.lo, col.hi, k)})
        if lo == hi:
            raise Skip
        return Between(x, lo, hi), "between"
    if k == "fk":
        if col.nullable and r < 0.4:
            return IsNull(x, neg=chance(0.5)), "null"
        if r < 0.75:
            return InList(x, sorted(rng.sample(range(1, int(col.hi) + 1), rng.randint(2, 4)))), "in"
        return Cmp(x, "=", Lit(rng.randint(1, int(col.hi)))), "cmp"
    if k == "cat":
        if col.nullable and r < 0.1:
            return IsNull(x, neg=chance(0.6)), "null"
        if r < 0.55:
            return Cmp(x, choice(["=", "=", "=", "<>"]), Lit(pool_val(col))), "cmp"
        neg = chance(0.25)
        pool = [v for v in col.pool if "'" not in v]
        # v5: a NOT IN over every value of the column always returned nothing
        vals = rng.sample(pool, min(len(pool) - (1 if neg else 0), rng.randint(2, 3)))
        if not vals:
            raise Skip
        return InList(x, vals, neg=neg), "in"
    if k == "str":
        v = pool_val(col).strip()
        if r < 0.45:
            if "@" in v:
                pat = "%" + v[v.index("@"):]
            elif chance(0.5):
                pat = v[: rng.randint(1, 3)] + "%"
            else:
                word = choice(w for w in re.split(r"[\s-]+", v) if len(w) >= 3)
                pat = "%" + word[: rng.randint(3, len(word))] + "%"
            return Like(x, pat, neg=chance(0.15)), "like"
        if r < 0.65:
            fn = choice([("LOWER", "lower"), ("UPPER", "upper")])
            lv = v.lower() if fn[1] == "lower" else v.upper()
            if chance(0.5):
                return Like(Fn(fn[0], fn[1], [x], kind="str"), "%" + lv[1:4] + "%"), "like"
            return Cmp(Fn(fn[0], fn[1], [Fn("TRIM", "trim", [x], kind="str")], kind="str"), "=", Lit(lv)), "func"
        if r < 0.8:
            return Cmp(Fn("LENGTH", "length", [x], kind="int"), choice([">", "<", ">="]),
                       Lit(rng.randint(5, 14))), "func"
        return IsNull(x, neg=chance(0.8)), "null"
    if k == "date":
        if r < 0.5:
            return Cmp(x, choice([">=", ">", "<", "<="]), Lit(rand_date())), "date"
        if r < 0.7:
            return Between(x, *rand_date_pair()), "between"
        if r < 0.85:
            return Cmp(Fn("YEAR", "year", [x], kind="int"), choice(["=", ">="]), Lit(rng.choice([2023, 2024, 2025]))), "func"
        return InList(Fn("MONTH", "month", [x], kind="int"), sorted(rng.sample(range(1, 13), 3))), "func"
    if k == "ts":
        if r < 0.5:
            return Cmp(x, choice([">=", "<"]), Lit(rand_date())), "date"
        if r < 0.75:
            lo = rng.randint(6, 12)
            return Between(Fn("HOUR", "hour", [x], kind="int"), lo, lo + rng.randint(3, 8)), "func"
        return Cmp(Fn("TO_DATE", "to_date", [x], kind="date"), choice([">=", "<"]), Lit(rand_date())), "func"
    if k == "bool":
        return BoolTest(x, neg=chance(0.35), explicit=chance(0.3)), "bool"
    if k == "array":
        if r < 0.7:
            return FnPred(Fn("ARRAY_CONTAINS", "array_contains", [x, Raw(pool_val(col))], kind="bool")), "array"
        return Cmp(Fn("SIZE", "size", [x], kind="int"), choice([">", ">="]), Lit(rng.randint(1, 2))), "array"
    raise Skip


def where(rels, feats, n=None):
    n = n or rng.choices([1, 2, 3], [0.45, 0.35, 0.2])[0]
    atoms = []
    for _ in range(n):
        p, f = atom(choice(rels))
        atoms.append(p)
        feats.add(f"where_{f}")
    if n == 1:
        p = atoms[0]
    elif n == 2:
        p = Or(atoms) if chance(0.25) else And(atoms)
    else:
        a, b, c = atoms
        p = wchoice([(And(atoms), 0.5), (And([a, Or([b, c])]), 0.3), (Or([And([a, b]), c]), 0.2)])
    if n > 1 and chance(0.06):
        p = Not(p)
        feats.add("where_not")
    if any(True for _ in find(p, Or)):
        feats.add("where_or")
    feats.add("where")
    return p


# ---------------------------------------------------------------------------
# Derived (scalar) expressions
# ---------------------------------------------------------------------------
NICE_PAIR_NAMES = {("quantity", "unit_price"): "line_total", ("price", "cost"): "margin",
                   ("revenue", "units"): "revenue_per_unit", ("amount", "shipping_fee"): "amount_with_shipping"}


def band(x, col, labels):
    """CASE WHEN x >= hi THEN .. WHEN x >= lo THEN .. ELSE .. END"""
    mid = col.lo + (col.hi - col.lo) * 0.5
    hi, lo = nice(mid, col.hi, col.kind), nice(col.lo, mid, col.kind)
    if hi <= lo:
        raise Skip
    return Case([(Cmp(x, ">=", Lit(hi)), Lit(labels[0])), (Cmp(x, ">=", Lit(lo)), Lit(labels[1]))], Lit(labels[2]))


def derive(rel, feats, col=None):
    col = col or choice(c for c in rel.cols if c.kind not in ("id", "fk"))
    x, n, k = rel.ref(col), col.name, col.kind
    opts: list[DeriveOpt] = []
    if k in ("int", "double"):
        others = [c for c in cols(rel, "int", "double") if c.name != n]
        m = rng.choice([1.07, 1.1, 1.15, 0.9, 0.85, 100])
        opts += [
            lambda: (Fn("ROUND", "round", [BinOp(x, "*", Lit(m)), Raw(2)]),
                     choice([f"{n}_adjusted", f"adjusted_{n}", f"{n}_scaled"]), "fn_num"),
            lambda: (Fn("ROUND", "round", [x, Raw(rng.choice([0, 1, 2]))]), f"{n}_rounded", "fn_num"),
            lambda: ((lambda f: Fn(f.upper(), f, [x], kind="int"))(rng.choice(["ceil", "floor"])),
                     f"{n}_whole", "fn_num"),
            lambda: (band(x, col, ("High", "Medium", "Low")), choice([f"{n}_band", f"{n}_level", f"{n}_tier"]),
                     "case"),
        ]
        if k == "double":
            opts.append(lambda: (Cast(x, "INT"), f"{n}_int", "cast"))
        if k == "int":
            opts.append(lambda: (Cast(x, rng.choice(["STRING", "DOUBLE"])), f"{n}_{'str' if chance(.5) else 'num'}", "cast"))
        if col.nullable:
            opts.append(lambda: (Fn("COALESCE", "coalesce", [x, Lit(0)]), f"{n}_filled", "coalesce"))
        if col.lo < 0:
            opts.append(lambda: (Fn("ABS", "abs", [x]), f"abs_{n}", "fn_num"))
        if others:
            y = choice(others)
            yx = rel.ref(y)
            nm = NICE_PAIR_NAMES.get((n, y.name)) or NICE_PAIR_NAMES.get((y.name, n))
            opts.append(lambda: (BinOp(x, "-", yx), nm or f"{n}_minus_{y.name}", "arith"))
            opts.append(lambda: (BinOp(x, "*", yx), nm or f"{n}_times_{y.name}", "arith"))
            opts.append(lambda: (BinOp(x, "+", yx), nm or f"{n}_plus_{y.name}", "arith"))
            if y.lo > 0:
                opts.append(lambda: (Fn("ROUND", "round", [BinOp(x, "/", yx), Raw(2)]),
                                     nm or f"{n}_per_{y.name}", "arith"))
            else:
                opts.append(lambda: (BinOp(x, "/", Fn("NULLIF", "nullif", [yx, Lit(0)])),
                                     f"{n}_per_{y.name}", "arith"))
            opts.append(lambda: (lambda f: (Fn(f.upper(), f, [x, yx]), f"{f}_{n}_{y.name}", "fn_num"))(
                rng.choice(["greatest", "least"])))
    elif k in ("cat", "str"):
        others = [c for c in cols(rel, "cat", "str") if c.name != n]
        opts += [
            lambda: (Fn("UPPER", "upper", [x], kind="str"), f"{n}_upper", "fn_str"),
            lambda: (Fn("LOWER", "lower", [x], kind="str"), f"{n}_lower", "fn_str"),
            lambda: (Fn("TRIM", "trim", [x], kind="str"), f"clean_{n}", "fn_str"),
            lambda: (Fn("LENGTH", "length", [x], kind="int"), f"{n}_length", "fn_str"),
            lambda: (Fn("SUBSTRING", "substring", [x, Raw(1), Raw(rng.randint(2, 4))], kind="str"),
                     f"{n}_prefix", "fn_str"),
            lambda: (Fn("INITCAP", "initcap", [Fn("TRIM", "trim", [x], kind="str")], kind="str"),
                     f"{n}_title", "fn_str"),
        ]
        if k == "str":
            opts.append(lambda: (Fn("UPPER", "upper", [Fn("TRIM", "trim", [x], kind="str")], kind="str"),
                                 f"{n}_normalized", "fn_str"))
            opts.append(lambda: (Fn(rng.choice(["REPLACE", "REGEXP_REPLACE"]), "regexp_replace",
                                    [x, Raw(" "), Raw(rng.choice(["_", "-", ""]))], kind="str"), f"{n}_slug", "fn_str"))
        if n == "email":
            opts += [
                lambda: (Fn("REGEXP_EXTRACT", "regexp_extract", [x, Raw("@(.+)$"), Raw(1)], kind="str"),
                         "email_domain", "fn_str"),
                lambda: (Fn("SPLIT", "split", [Fn("LOWER", "lower", [x], kind="str"), Raw("@")], kind="str",
                            suffix_sql="[0]", suffix_py=".getItem(0)"), "email_user", "fn_str"),
            ]
        if col.nullable:
            opts.append(lambda: (Fn("COALESCE", "coalesce", [x, Lit(rng.choice(["Unknown", "N/A", "unassigned"]))],
                                    kind="str"), f"{n}_filled", "coalesce"))
        if k == "cat" and len(col.pool) >= 3:
            vals = rng.sample(list(col.pool), 2)
            label = rng.choice(["Group A", "Priority", "Core", "Tier 1"])
            opts.append(lambda: (Case([(InList(x, vals), Lit(label))], Lit("Other")), f"{n}_group", "case"))
            v = pool_val(col)
            opts.append(lambda: (Case([(Cmp(x, "=", Lit(v)), Lit(1))], Lit(0), kind="int"), f"is_{ident(v)}", "case"))
        if others:
            y = rel.ref(choice(others))
            sep = rng.choice([" - ", ", ", " ", "/"])
            opts.append(lambda: (Fn("CONCAT", "concat", [x, Lit(sep), y], kind="str"), f"{n}_{y.name}", "fn_str"))
            opts.append(lambda: (Fn("CONCAT_WS", "concat_ws", [Raw(sep.strip() or "-"), x, y], kind="str"),
                                 f"{n}_{y.name}_key", "fn_str"))
    elif k in ("date", "ts"):
        parts = [("YEAR", "year"), ("MONTH", "month"), ("QUARTER", "quarter"), ("DAYOFWEEK", "dayofweek"),
                 ("DAY", "dayofmonth"), ("WEEKOFYEAR", "weekofyear")]
        opts += [
            lambda: (lambda p: (Fn(p[0], p[1], [x], kind="int"), f"{n}_{p[1].replace('dayofmonth', 'day')}",
                                "fn_date"))(rng.choice(parts)),
            lambda: (Fn("DATE_FORMAT", "date_format", [x, Raw(rng.choice(["yyyy-MM", "yyyy-MM-dd", "MMM yyyy"]))],
                        kind="str"), f"{n}_label", "fn_date"),
            lambda: (Fn("DATE_TRUNC", "date_trunc", [Raw(rng.choice(["MONTH", "YEAR", "WEEK"]) if chance(.5)
                                                          else rng.choice(["month", "year", "week"])), x], kind="ts"),
                     f"{n}_period", "fn_date"),
            lambda: (Fn("DATEDIFF", "datediff", [NoArgFn("CURRENT_DATE", "current_date"), x], kind="int"),
                     f"days_since_{n}", "fn_date"),
        ]
        if k == "date":
            d = rng.randint(1, 90)
            opts += [
                lambda: (lambda f: (Fn(f.upper(), f, [x, Raw(d)], kind="date"),
                                    f"{n}_{'plus' if f == 'date_add' else 'minus'}_{d}d", "fn_date"))(
                    rng.choice(["date_add", "date_sub"])),
                lambda: (Fn("ADD_MONTHS", "add_months", [x, Raw(rng.choice([1, 3, 6, 12]))], kind="date"),
                         f"{n}_plus_months", "fn_date"),
                lambda: (Fn("LAST_DAY", "last_day", [x], kind="date"), f"{n}_month_end", "fn_date"),
            ]
            others = [c for c in cols(rel, "date") if c.name != n]
            if others:
                y = rel.ref(choice(others))
                opts.append(lambda: (Fn("DATEDIFF", "datediff", [y, x], kind="int"), f"days_{n}_to_{y.name}",
                                     "fn_date"))
        else:
            opts += [
                lambda: (Fn("HOUR", "hour", [x], kind="int"), f"{n}_hour", "fn_date"),
                lambda: (Fn("TO_DATE", "to_date", [x], kind="date"), f"{n}_date", "fn_date"),
                lambda: (Cast(x, "DATE"), f"{n}_day", "cast"),
            ]
    elif k == "bool":
        opts += [
            lambda: (Case([(BoolTest(x), Lit("Yes"))], Lit("No"), if_form=chance(0.5)), f"{n}_label", "case"),
            lambda: (Cast(x, "INT"), f"{n}_flag", "cast"),
        ]
    elif k == "array":
        opts += [
            lambda: (Fn("SIZE", "size", [x], kind="int"), f"{n}_count", "array"),
            lambda: (Fn("ARRAY_JOIN", "array_join", [x, Raw(rng.choice([", ", "|"]))], kind="str"), f"{n}_text",
                     "array"),
            lambda: (Fn("SORT_ARRAY", "sort_array", [x], kind="array"), f"sorted_{n}", "array"),
            lambda: (lambda v: (Fn("ARRAY_CONTAINS", "array_contains", [x, Raw(v)], kind="bool"), f"is_{ident(v)}",
                                "array"))(pool_val(col)),
        ]
    if not opts:
        raise Skip
    e, name, f = choice(opts)()
    feats.add(f)
    return e, name


# ---------------------------------------------------------------------------
# Aggregates / group keys
# ---------------------------------------------------------------------------
def make_agg(rel, feats, table_name=None):
    tname = table_name or rel.sql_name
    nums = [c for c in rel.cols if c.kind in ("int", "double")]
    cats = [c for c in rel.cols if c.kind == "cat"]
    dates = [c for c in rel.cols if c.kind in ("date", "ts")]
    opts: list[AggOpt] = [lambda: (Agg("COUNT", kind="int"), choice(["cnt", f"{singular(tname)}_count", f"num_{tname}",
                                                         "total_count", "row_count"]))]
    keyish = [c for c in rel.cols if c.kind in ("fk", "cat")]
    if keyish:
        c = choice(keyish)
        stem = c.name[:-3] + "s" if c.name.endswith("_id") else c.name + "s"
        opts.append(lambda: (Agg("COUNT", rel.ref(c), distinct=True, kind="int"), f"unique_{stem}"))
    if nums:
        c = choice(nums)
        x = rel.ref(c)
        opts += [
            lambda: (Agg("SUM", x), f"total_{c.name}"),
            lambda: (Agg("AVG", x), f"avg_{c.name}"),
            lambda: (Fn("ROUND", "round", [Agg("AVG", x), Raw(2)]), f"avg_{c.name}"),
            lambda: (Agg("MAX", x), f"max_{c.name}"),
            lambda: (Agg("MIN", x), f"min_{c.name}"),
            lambda: (Fn("ROUND", "round", [Agg("STDDEV", x), Raw(2)]), f"stddev_{c.name}"),
            lambda: (BinOp(Agg("MAX", x), "-", Agg("MIN", x)), f"{c.name}_range"),
        ]
        if c.nullable:
            opts.append(lambda: (Agg("COUNT", x, kind="int"), f"{c.name}_count"))
        if cats:
            cc = choice(cats)
            v = pool_val(cc)
            cx = rel.ref(cc)
            opts += [
                lambda: (Agg("SUM", Case([(Cmp(cx, "=", Lit(v)), x)], Lit(0), kind="double")),
                         f"{ident(v)}_{c.name}"),
                lambda: (Fn("ROUND", "round", [BinOp(BinOp(Lit(100.0), "*", Agg(
                    "SUM", Case([(Cmp(cx, "=", Lit(v)), Lit(1))], Lit(0), kind="int"))), "/", Agg("COUNT", kind="int")),
                    Raw(1)]), f"pct_{ident(v)}"),
            ]
        others = [o for o in nums if o.name != c.name]
        if others:
            o = choice(others)
            nm = NICE_PAIR_NAMES.get((c.name, o.name)) or NICE_PAIR_NAMES.get((o.name, c.name))
            if nm and nm in ("line_total",):
                opts.append(lambda: (Agg("SUM", BinOp(x, "*", rel.ref(o))), "revenue"))
    if cats:
        cc = choice(cats)
        v = pool_val(cc)
        cx = rel.ref(cc)
        opts += [
            lambda: (Agg("COUNT", Case([(Cmp(cx, "=", Lit(v)), Lit(1))], kind="int"), kind="int"), f"{ident(v)}_count"),
            lambda: (Agg("SUM", Case([(Cmp(cx, "=", Lit(v)), Lit(1))], Lit(0), kind="int"), kind="int"),
                     f"{ident(v)}_count"),
            lambda: (Fn("SORT_ARRAY", "sort_array", [Agg("COLLECT_SET", cx)], kind="array"), f"{cc.name}_list"),
            lambda: (Fn("CONCAT_WS", "concat_ws", [Raw(", "), Fn("SORT_ARRAY", "sort_array",
                                                                   [Agg("COLLECT_SET", cx)], kind="array")], kind="str"),
                     f"{cc.name}_values"),
        ]
    if dates:
        d = choice(dates)
        opts += [lambda: (Agg("MIN", rel.ref(d), kind="date"), choice([f"first_{d.name}", f"min_{d.name}"])),
                 lambda: (Agg("MAX", rel.ref(d), kind="date"), choice([f"last_{d.name}", f"latest_{d.name}"]))]
    e, name = choice(opts)()
    feats.add("agg_case" if "Case" in type(getattr(e, "e", None)).__name__ else "agg")
    return e, name


def make_aggs(rel, feats, n, table_name=None, exclude=()):
    out, seen = [], set(exclude)
    for _ in range(n * 3):
        if len(out) >= n:
            break
        e, name = make_agg(rel, feats, table_name)
        if name not in seen:
            seen.add(name)
            out.append((e, name))
    return out


def make_key(rel, feats):
    catlike = [c for c in rel.cols if c.kind in ("cat", "bool")]
    dates = [c for c in rel.cols if c.kind in ("date", "ts")]
    nums = [c for c in rel.cols if c.kind in ("int", "double")]
    fk = [c for c in rel.cols if c.kind == "fk"]
    if chance(0.12):
        e, name = case_key(rel)
        feats.add("group_case")
        return e, name
    r = rng.random()
    if catlike and r < 0.62:
        c = choice(catlike)
        return rel.ref(c), c.name
    if fk and r < 0.72:
        c = choice(fk)
        return rel.ref(c), c.name
    if dates and r < 0.95:
        d = choice(dates)
        x = rel.ref(d)
        feats.add("group_expr")
        prefix = d.name.replace("_date", "").replace("_time", "").replace("_at", "")
        return choice([
            (Fn("YEAR", "year", [x], kind="int"), f"{prefix}_year"),
            (Fn("MONTH", "month", [x], kind="int"), f"{prefix}_month"),
            (Fn("DATE_FORMAT", "date_format", [x, Raw("yyyy-MM")], kind="str"), f"{prefix}_month"),
            (Fn("DATE_TRUNC", "date_trunc", [Raw("MONTH"), x], kind="ts"), f"{prefix}_month"),
            (Fn("QUARTER", "quarter", [x], kind="int"), f"{prefix}_quarter"),
        ] + ([(Fn("HOUR", "hour", [x], kind="int"), f"{prefix}_hour"),
              (Fn("TO_DATE", "to_date", [x], kind="date"), f"{prefix}_day")] if d.kind == "ts" else []))
    if nums:
        c = choice(nums)
        feats.add("group_expr")
        return band(rel.ref(c), c, ("high", "mid", "low")), f"{c.name}_bucket"
    if catlike:
        c = choice(catlike)
        return rel.ref(c), c.name
    raise Skip


def case_key(rel):
    """A CASE expression used as a GROUP BY key (numeric bucket, category group or date cohort)."""
    cats = [c for c in rel.cols if c.kind == "cat" and len(c.pool) >= 3]
    dates = [c for c in rel.cols if c.kind in ("date", "ts")]
    nums = [c for c in rel.cols if c.kind in ("int", "double")]
    opts = []
    if cats:
        c = choice(cats)
        vals = rng.sample([v for v in c.pool if "'" not in v], 2)
        label = rng.choice(["core", "priority", "group_a", "tier_1"])
        opts.append(lambda c=c: (Case([(InList(rel.ref(c), vals), Lit(label))], Lit("other")), f"{c.name}_group"))
    if dates:
        d = choice(dates)
        y = Fn("YEAR", "year", [rel.ref(d)], kind="int")
        prefix = d.name.replace("_date", "").replace("_time", "").replace("_at", "")
        opts.append(lambda: (Case([(Cmp(y, "<", Lit(2024)), Lit("before_2024")), (Cmp(y, "=", Lit(2024)), Lit("2024"))],
                                  Lit("after_2024")), f"{prefix}_cohort"))
    if nums:
        n = choice(nums)
        opts.append(lambda: (band(rel.ref(n), n, ("high", "mid", "low")), f"{n.name}_bucket"))
    return choice(opts)()


def make_keys(rel, feats, n):
    out, seen = [], set()
    for _ in range(n * 4):
        if len(out) >= n:
            break
        e, name = make_key(rel, feats)
        if name not in seen and not any(e.sql is o.sql for o, _ in out):
            seen.add(name)
            out.append((e, name))
    if len(out) < n:
        raise Skip
    return out


def having_value(e, name):
    col = out_columns(Sel(Rel("x", "x", []), aggs=[(e, name)], group=[]))[0]
    if isinstance(e, Agg) and e.func == "COUNT":
        return rng.randint(1, 5)
    return nice(col.lo, col.hi, col.kind)


# ---------------------------------------------------------------------------
# ORDER BY / LIMIT
# ---------------------------------------------------------------------------
def add_order(q, st, feats, p=0.5, limit_p=0.25, unique=None, orderable=None):
    if not chance(p):
        return
    outs = q.out_cols()
    names = [n for n, e in outs if e.kind not in ("array", "bool") and (orderable is None or n in orderable)]
    if not names:
        return
    k = min(len(names), rng.choices([1, 2], [0.75, 0.25])[0])
    picked = rng.sample(names, k)
    order = []
    for n in picked:
        e = dict(outs)[n]
        desc = chance(0.65 if e.kind in ("int", "double") else 0.3)
        sql_text = None
        if isinstance(e, Col) and e.q and chance(0.4):
            sql_text = e.sql(st)
        elif chance(0.08):
            sql_text = str(q.out_names().index(n) + 1)
        order.append((n, desc, sql_text))
    feats.add("order")
    if unique is not None and chance(limit_p):
        # LIMIT must be deterministic: make sure a unique key completes the sort
        for u in unique:
            if u not in [o[0] for o in order]:
                if u not in q.out_names():
                    q.order = order
                    return
                order.append((u, False, None))
        q.limit = rng.choice([1, 3, 5, 5, 10, 10, 20, 50])
        feats.add("limit")
    q.order = order


def uniq_items(items, avoid=()):
    seen, out = set(avoid), []
    for e, n in items:
        base, i = n, 2
        while n in seen:
            n, i = f"{base}_{i}", i + 1
        seen.add(n)
        out.append((e, n))
    return out


def with_sql(ctes, main_sql, st):
    k = st.k
    if st.multiline:
        body = ",\n".join(f"{name} {k('AS')} (\n    {sql}\n)" for name, sql in ctes)
        return f"{k('WITH')} {body}\n{main_sql}"
    return f"{k('WITH')} " + ", ".join(f"{name} {k('AS')} ({sql})" for name, sql in ctes) + " " + main_sql


def cte_name(st, base):
    if chance(0.15):
        return "".join(w.capitalize() for w in base.split("_"))
    return base


# ---------------------------------------------------------------------------
# Families. Each returns (sql, defs, result_chain, feats, order_cols)
# ---------------------------------------------------------------------------
def fam_select(st, ctx):
    t = choice(TABLES)
    rel = base_rel(t)
    feats = {"select"}
    q = Sel(rel)
    unique = None
    if chance(0.15):
        q.star = True
        feats.add("star")
        if chance(0.6):
            q.items = uniq_items([derive(rel, feats) for _ in range(rng.randint(1, 2))], avoid=[c.name for c in t.columns])
        if not q.items or chance(0.8):
            q.where = where([rel], feats)
        unique = ["id"]
    elif chance(0.08):
        cs = rng.sample(cols(rel, "cat"), min(2, len(cols(rel, "cat")), rng.randint(1, 2))) if cols(rel, "cat") else []
        if not cs:
            raise Skip
        q.items = [(Col(c), c.name) for c in cs]
        q.distinct = True
        feats.add("distinct")
        if chance(0.5):
            q.where = where([rel], feats, n=1)
        unique = [n for _, n in q.items]
    else:
        items: list[tuple[E, str]] = [(Col(t.col("id")), "id")] if chance(0.65) else []
        pool = [c for c in t.columns if c.kind != "id"]
        items += [(Col(c), c.name) for c in rng.sample(pool, min(len(pool), rng.randint(1, 3)))]
        for _ in range(rng.choices([0, 1, 2], [0.35, 0.45, 0.2])[0]):
            items.append(derive(rel, feats))
        q.items = uniq_items(items)
        if chance(0.75):
            q.where = where([rel], feats)
        unique = ["id"]
    add_order(q, st, feats, p=0.4, limit_p=0.3, unique=unique)
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def fam_agg(st, ctx):
    t = choice(t for t in TABLES if any(c.kind in ("int", "double") for c in t.columns))
    rel = base_rel(t)
    feats = {"agg"}
    nkeys = rng.choices([0, 1, 2, 3], [0.1, 0.55, 0.3, 0.05])[0]
    q = Sel(rel)
    q.group = make_keys(rel, feats, nkeys) if nkeys else None
    if q.group:
        feats.add("group")
        has_expr = any(not isinstance(e, Col) for e, _ in q.group)
        has_case = any(isinstance(e, Case) for e, _ in q.group)
        q.group_ref = wchoice([("expr", .4), ("alias", .25), ("ordinal", .35)] if has_case else
                              [("expr", .6), ("alias", .2), ("ordinal", .2)] if has_expr else
                              [("expr", .85), ("ordinal", .15)])
    q.aggs = make_aggs(rel, feats, rng.choices([1, 2, 3], [0.4, 0.4, 0.2])[0], exclude=[n for _, n in q.group or []])
    if chance(0.55):
        q.where = where([rel], feats)
    if q.group and chance(0.35):
        feats.add("having")
        if chance(0.3):
            q.hidden = [x for x in make_aggs(rel, set(), 1, exclude=[n for _, n in q.aggs + q.group])
                        if isinstance(x[0], Agg) and x[0].kind in ("int", "double")]
        src = q.hidden or [a for a in q.aggs if a[0].kind in ("int", "double")]
        if src:
            e, n = choice(src)
            q.having = [(e, n, choice([">", ">=", "<"]), having_value(e, n), not q.hidden and chance(0.35))]
    if nkeys == 2 and not q.having and chance(0.12):
        q.group_mode = choice(["rollup", "cube"])
        q.group_ref = "expr"
        feats.add(q.group_mode)
    add_order(q, st, feats, p=0.55, limit_p=0.25,
              unique=[n for _, n in q.group] if q.group and q.group_mode == "plain" else None)
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def join_rels(st, feats):
    """Pick a join shape. Returns (rels, joins, shape)."""
    shape = wchoice([("child_parent", .38), ("two_parents", .14), ("chain", .16), ("parent_child", .18),
                     ("self", .07), ("multikey", .07)])
    if shape == "self":
        t = TABLE_BY_NAME["employees"]
        a, b = ("e", "m") if chance(.7) else ("emp", "mgr")
        r1, r2 = base_rel(t, a), base_rel(t, b)
        cond = Cmp(r1.ref(t.col("manager_id")), "=", r2.ref(t.col("id")))
        feats.add("self_join")
        return [r1, r2], [Join(r2, choice(["left", "left", "inner"]), cond)], shape
    if shape == "multikey":
        t1, t2 = TABLE_BY_NAME["inventory"], TABLE_BY_NAME["store_sales"]
        a1, a2 = aliases_for(st, [t1.name, t2.name])
        r1, r2 = base_rel(t1, a1), base_rel(t2, a2)
        cond = And([Cmp(r1.ref(t1.col(k)), "=", r2.ref(t2.col(k))) for k in ("store_id", "product_id")])
        feats.add("multikey_join")
        return [r1, r2], [Join(r2, choice(["inner", "left"]), cond)], shape
    if shape == "parent_child":
        parent = choice(t for t in TABLES if children(t))
        child, fk = choice(children(parent))
        a1, a2 = aliases_for(st, [parent.name, child.name])
        r1, r2 = base_rel(parent, a1), base_rel(child, a2)
        cond = Cmp(r1.ref(parent.col("id")), "=", r2.ref(fk)) if chance(.5) else Cmp(r2.ref(fk), "=", r1.ref(parent.col("id")))
        return [r1, r2], [Join(r2, wchoice([("left", .75), ("inner", .25)]), cond)], shape
    child = choice(t for t in TABLES if fks(t) and (shape != "two_parents" or len(fks(t)) >= 2)
                   and (shape != "chain" or any(fks(TABLE_BY_NAME[f.ref]) for f in fks(t))))
    if shape == "child_parent":
        steps = [(0, choice(fks(child)))]
        tables = [child, TABLE_BY_NAME[steps[0][1].ref]]
    elif shape == "two_parents":
        f1, f2 = rng.sample(fks(child), 2)
        steps = [(0, f1), (0, f2)]
        tables = [child, TABLE_BY_NAME[f1.ref], TABLE_BY_NAME[f2.ref]]
    else:
        f1 = choice(f for f in fks(child) if fks(TABLE_BY_NAME[f.ref]))
        mid = TABLE_BY_NAME[f1.ref]
        f2 = choice(fks(mid))
        steps = [(0, f1), (1, f2)]
        tables = [child, mid, TABLE_BY_NAME[f2.ref]]
    if len({t.name for t in tables}) < len(tables):
        raise Skip
    names = aliases_for(st, [t.name for t in tables])
    rels = [base_rel(t, a) for t, a in zip(tables, names)]
    joins = []
    for i, (src, fk) in enumerate(steps):
        parent = rels[i + 1]
        assert parent.table is not None
        how = wchoice([("inner", .55), ("left", .3), ("right", .08), ("full", .07)])
        l, r = rels[src].ref(fk), parent.ref(parent.table.col("id"))
        cond = Cmp(l, "=", r) if chance(.75) else Cmp(r, "=", l)
        joins.append(Join(parent, how, cond))
    return rels, joins, shape


def join_items(rels, feats, shape):
    items, seen = [], set()
    for i, r in enumerate(rels):
        pool = [c for c in r.cols if c.kind not in ("array",)]
        if i == 0 and shape not in ("parent_child",):
            picks = [r.table.col("id")] if chance(.6) else []
        else:
            picks = []
        k = rng.randint(1, 2 if len(rels) > 2 else 3)
        picks += rng.sample([c for c in pool if c.kind != "id"], k)
        for c in picks:
            name = c.name
            if shape == "self":
                name = f"{'employee' if i == 0 else 'manager'}_{c.name}" if c.name != "id" or i else "id"
            if name in seen:
                name = f"{singular(r.table.name)}_{c.name}"
            seen.add(name)
            items.append((r.ref(c), name))
    if chance(0.2):
        e, n = derive(choice(rels), feats)
        if n not in seen:
            items.append((e, n))
    return items


def fam_join(st, ctx):
    feats = {"join"}
    rels, joins, shape = join_rels(st, feats)
    feats |= {f"join_{j.how}" for j in joins}
    if len(joins) > 1:
        feats.add("multi_join")
    q = Sel(rels[0], joins=joins)
    q.items = join_items(rels, feats, shape)
    if chance(0.6):
        q.where = where(rels, feats, n=rng.choices([1, 2], [.6, .4])[0])
    unique = ["id"] if shape in ("child_parent", "two_parents", "chain", "self") and all(
        j.how in ("inner", "left") for j in joins) else None
    add_order(q, st, feats, p=0.45, limit_p=0.25, unique=unique)
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def fam_join_agg(st, ctx):
    feats = {"join", "agg", "group"}
    rels, joins, shape = join_rels(st, feats)
    if shape == "self":
        raise Skip
    feats |= {f"join_{j.how}" for j in joins}
    q = Sel(rels[0], joins=joins)
    if shape == "parent_child":
        parent, child = rels
        keys = [(parent.ref(parent.table.col("id")), "id")]
        label = [c for c in parent.cols if c.kind in ("str", "cat")]
        if label and chance(.8):
            c = choice(label)
            keys.append((parent.ref(c), c.name))
        if chance(.3):
            keys = [(parent.ref(c), c.name) for c in rng.sample(cols(parent, "cat"), 1)] if cols(parent, "cat") else keys
        q.group = keys
        aggs = [(Agg("COUNT", child.ref(child.table.col("id")), kind="int"), f"{singular(child.table.name)}_count")]
        aggs += make_aggs(child, feats, rng.randint(0, 2), table_name=child.table.name, exclude=[aggs[0][1]])
        q.aggs = [(e, n) for e, n in aggs if not (isinstance(e, Agg) and e.func == "COUNT" and e.e is None)]
    else:
        parents = rels[1:]
        keysrc = choice(parents)
        q.group = make_keys(keysrc, feats, rng.choices([1, 2], [.7, .3])[0])
        q.aggs = make_aggs(rels[0], feats, rng.randint(1, 2), table_name=rels[0].table.name,
                           exclude=[n for _, n in q.group])
    if not q.aggs:
        raise Skip
    if chance(0.45):
        q.where = where(rels, feats, n=1)
    if chance(0.25):
        e, n = choice(q.aggs)
        if e.kind in ("int", "double"):
            q.having = [(e, n, choice([">", ">="]), having_value(e, n), chance(.3))]
            feats.add("having")
    add_order(q, st, feats, p=0.6, limit_p=0.3, unique=[n for _, n in q.group])
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def make_window(rel, feats):
    parts_pool = [c for c in rel.cols if c.kind in ("cat", "fk")]
    order_pool = [c for c in rel.cols if c.kind in ("int", "double", "date", "ts")]
    num_pool = [c for c in rel.cols if c.kind in ("int", "double") and c.lo >= 0]
    if not order_pool:
        raise Skip
    idc = Col(rel.table.col("id")) if rel.table else None
    npart = rng.choices([0, 1, 2], [.15, .75, .1])[0]
    part = [Col(c) for c in rng.sample(parts_pool, min(npart, len(parts_pool)))]
    oc = choice(order_pool)
    desc = chance(.6 if oc.kind in ("int", "double") else .3)
    tiebreak = [(idc, False)] if idc is not None else []
    kind = wchoice([("rank", .35), ("lag", .2), ("part_agg", .15), ("running", .15), ("first_last", .07),
                    ("share", .08)])
    feats.add(f"window_{kind}")
    if kind == "rank":
        fn = wchoice([(("ROW_NUMBER", "row_number"), .35), (("RANK", "rank"), .25), (("DENSE_RANK", "dense_rank"), .2),
                      (("NTILE", "ntile"), .1), (("PERCENT_RANK", "percent_rank"), .1)])
        args = [Raw(rng.choice([2, 3, 4, 10]))] if fn[0] == "NTILE" else []
        order = [(Col(oc), desc)] + (tiebreak if fn[0] in ("ROW_NUMBER", "NTILE") else [])
        name = {"ROW_NUMBER": choice(["rn", "row_num", f"{oc.name}_row"]), "RANK": choice(["rnk", f"{oc.name}_rank"]),
                "DENSE_RANK": choice(["dense_rnk", f"{oc.name}_dense_rank"]),
                "NTILE": choice(["bucket", f"{oc.name}_ntile", "quartile"]),
                "PERCENT_RANK": f"{oc.name}_pct_rank"}[fn[0]]
        return WinFn(fn[0], fn[1], args, WinSpec(part, order), kind="double" if fn[0] == "PERCENT_RANK" else "int"), name
    if kind == "lag":
        dates = [c for c in rel.cols if c.kind in ("date", "ts")] or order_pool
        by = choice(dates)
        val = choice(num_pool or order_pool)
        fn = choice([("LAG", "lag"), ("LEAD", "lead")])
        off = rng.choice([1, 1, 1, 2])
        args = [Col(val), Raw(off)] + ([Raw(0)] if val.kind in ("int", "double") and chance(.2) else [])
        spec = WinSpec(part, [(Col(by), False)] + tiebreak)
        w = WinFn(fn[0], fn[1], args, spec, kind=val.kind if val.kind in ("int", "double") else "date")
        if val.kind in ("int", "double") and fn[0] == "LAG" and chance(.35):
            return BinOp(Col(val), "-", w), f"{val.name}_change"
        return w, f"{'prev' if fn[0] == 'LAG' else 'next'}_{val.name}"
    if kind in ("part_agg", "share"):
        if not part or not num_pool:
            raise Skip
        val = choice(c for c in num_pool if c.lo > 0 or kind != "share")
        if kind == "share":
            return (Fn("ROUND", "round", [BinOp(Col(val), "/", WinFn("SUM", "sum", [Col(val)], WinSpec(part))), Raw(3)]),
                    f"{val.name}_share")
        fn = choice([("SUM", "sum"), ("AVG", "avg"), ("MAX", "max"), ("MIN", "min"), ("COUNT", "count")])
        if fn[0] == "COUNT":
            return WinFn(fn[0], fn[1], [Star()], WinSpec(part)), f"{part[0].name}_count"
        return WinFn(fn[0], fn[1], [Col(val)], WinSpec(part)), f"{part[0].name}_{fn[1]}_{val.name}"
    if kind == "running":
        if not num_pool:
            raise Skip
        val = choice(num_pool)
        by = choice([c for c in rel.cols if c.kind in ("date", "ts")] or order_pool)
        spec_order = [(Col(by), False)] + tiebreak
        if chance(.4):
            n = rng.choice([2, 3, 6])
            spec = WinSpec(part, spec_order, (-n, "CR"))
            feats.add("window_frame")
            return (Fn("ROUND", "round", [WinFn("AVG", "avg", [Col(val)], spec, kind="double"), Raw(2)]),
                    f"{val.name}_moving_avg_{n + 1}")
        frame = ("UP", "CR") if chance(.5) else None
        if frame:
            feats.add("window_frame")
        return WinFn("SUM", "sum", [Col(val)], WinSpec(part, spec_order, frame), kind="double"), f"running_{val.name}"
    # first_last
    used = {p.name for p in part} | {oc.name}
    val = choice(c for c in rel.cols if c.kind not in ("array", "id", "bool") and c.name not in used)
    order = [(Col(oc), desc)] + tiebreak
    if chance(.5):
        return WinFn("FIRST_VALUE", "first", [Col(val)], WinSpec(part, order), kind=Col(val).kind), f"first_{val.name}"
    feats.add("window_frame")
    return (WinFn("LAST_VALUE", "last", [Col(val)], WinSpec(part, order, ("UP", "UF")), kind=Col(val).kind),
            f"last_{val.name}")


def window_items(rel, wins):
    """Bare columns that make the window output readable (partition, order and value columns)."""
    names = ["id"]
    for e, _ in wins:
        for w in find(e, WinFn):
            names += [p.name for p in w.spec.part] + [o.name for o, _ in w.spec.order]
            names += [a.name for a in w.args if isinstance(a, Col)]
    return [(Col(rel.table.col(n)), n) for n in dict.fromkeys(names)]


def fam_window(st, ctx):
    t = choice(t for t in TABLES if any(c.kind in ("cat", "fk") for c in t.columns))
    rel = base_rel(t)
    feats = {"window"}
    wins = uniq_items([make_window(rel, feats) for _ in range(rng.choices([1, 2], [.7, .3])[0])],
                      avoid=[c.name for c in t.columns])
    q = Sel(rel, items=window_items(rel, wins) + wins)
    if chance(0.4):
        q.where = where([rel], feats, n=1)
    add_order(q, st, feats, p=0.3, limit_p=0)
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def fam_topn(st, ctx):
    t = choice(t for t in TABLES if any(c.kind in ("cat", "fk") for c in t.columns))
    rel = base_rel(t)
    feats = {"window", "top_n"}
    part_c = choice(c for c in t.columns if c.kind in ("cat", "fk"))
    oc = choice(c for c in t.columns if c.kind in ("int", "double", "date", "ts"))
    fn = wchoice([(("ROW_NUMBER", "row_number"), .7), (("RANK", "rank"), .15), (("DENSE_RANK", "dense_rank"), .15)])
    desc = chance(.7)
    order = [(Col(oc), desc)] + ([(Col(t.col("id")), False)] if fn[0] == "ROW_NUMBER" else [])
    rn = choice(["rn", "rnk", "row_num", "pos"])
    win = (WinFn(fn[0], fn[1], [], WinSpec([Col(part_c)], order)), rn)
    inner = Sel(rel)
    keep = [(Col(t.col("id")), "id"), (Col(part_c), part_c.name), (Col(oc), oc.name)]
    extra = [c for c in t.columns if c.name not in {"id", part_c.name, oc.name} and c.kind != "array"]
    if extra and chance(.6):
        c = choice(extra)
        keep.append((Col(c), c.name))
    if chance(.5):
        inner.star, inner.items = True, [win]
    else:
        inner.items = keep + [win]
    if chance(.3):
        inner.where = where([rel], feats, n=1)
    use_cte = chance(.6)
    name = cte_name(st, choice(["ranked", f"ranked_{t.name}", "numbered", f"top_{t.name}"]))
    sub = Rel(name, name, out_columns(inner), sub=inner, sub_sql_inline=not use_cte, py_inline=True,
              table=t)
    if not use_cte:
        sub.sql_alias = choice(["t", "r", "ranked"])
    outer = Sel(sub)
    n = rng.choice([1, 1, 2, 3, 3, 5])
    rncol = Col(sub.col(rn))
    outer.where = Cmp(rncol, "=", Lit(1)) if n == 1 and chance(.7) else Cmp(rncol, "<=", Lit(n))
    if chance(.35):
        outer.star = True
    else:
        outer.items = [(Col(sub.col(nm)), nm) for _, nm in keep]
    add_order(outer, st, feats, p=0.5, limit_p=0)
    feats.add("cte" if use_cte else "derived")
    main_sql = outer.sql(st)
    sql = with_sql([(name, inner.sql(st, "    " if st.multiline else ""))], main_sql, st) if use_cte else main_sql
    return sql, [], outer.py_chain(ctx), feats, [o[0] for o in outer.order]


def fam_cte_agg_join(st, ctx):
    parent = choice(t for t in TABLES if children(t))
    child, fk = choice(children(parent))
    feats = {"cte", "agg", "group", "join"}
    crel = base_rel(child)
    inner = Sel(crel, group=[(Col(fk), fk.name)])
    inner.aggs = make_aggs(crel, feats, rng.randint(1, 2), table_name=child.name, exclude=[fk.name])
    if chance(.4):
        inner.where = where([crel], feats, n=1)
    pname = singular(parent.name)
    name = cte_name(st, choice([f"{pname}_totals", f"{pname}_stats", f"{child.name}_by_{pname}", f"{pname}_summary"]))
    derived = chance(.25)
    pa, ca = aliases_for(st, [parent.name, name if not derived else "t"])
    if not st.short_alias and not derived:
        ca = name
    sub = Rel(name, snake(name), out_columns(inner), alias=ca, sub=inner, sub_sql_inline=derived)
    prel = base_rel(parent, pa)
    how = wchoice([("inner", .6), ("left", .4)])
    cond = Cmp(prel.ref(parent.col("id")), "=", sub.ref(sub.col(fk.name)))
    q = Sel(prel, joins=[Join(sub, how, cond)])
    feats.add(f"join_{how}")
    pcols = [c for c in parent.columns if c.kind not in ("array", "id")]
    items = [(prel.ref(parent.col("id")), "id")] if chance(.6) else []
    items += [(prel.ref(c), c.name) for c in rng.sample(pcols, min(len(pcols), rng.randint(1, 2)))]
    items += [(sub.ref(sub.col(n)), n) for _, n in inner.aggs]
    q.items = uniq_items(items)
    if chance(.5):
        aggcols = [sub.col(n) for e, n in inner.aggs if e.kind in ("int", "double")]
        if aggcols:
            c = choice(aggcols)
            q.where = Cmp(sub.ref(c), choice([">", ">="]), Lit(nice(c.lo, c.hi, c.kind)))
            feats.add("where")
    add_order(q, st, feats, p=0.6, limit_p=0.35, unique=["id"] if how == "inner" else None)
    main_sql = q.sql(st)
    defs = [(snake(name), inner.py_chain(ctx))]
    sql = main_sql if derived else with_sql([(name, inner.sql(st, "    " if st.multiline else ""))], main_sql, st)
    if derived:
        feats.discard("cte")
        feats.add("derived")
    return sql, defs, q.py_chain(ctx), feats, [o[0] for o in q.order]


def fam_cte_chain(st, ctx):
    t = choice(t for t in TABLES if any(c.kind in ("int", "double") for c in t.columns))
    rel = base_rel(t)
    feats = {"cte", "cte_chain", "agg", "group"}
    s1 = Sel(rel, star=True)
    s1.where = where([rel], feats)
    if chance(.3):
        s1.items = uniq_items([derive(rel, feats)], avoid=[c.name for c in t.columns])
    n1 = cte_name(st, choice([f"filtered_{t.name}", f"recent_{t.name}", f"valid_{t.name}", "base", "src"]))
    r1 = Rel(n1, snake(n1), out_columns(s1), sub=s1)
    s2 = Sel(r1)
    s2.group = make_keys(r1, feats, rng.choices([1, 2], [.7, .3])[0])
    s2.aggs = make_aggs(r1, feats, rng.randint(1, 2), table_name=t.name, exclude=[n for _, n in s2.group])
    n2 = cte_name(st, choice([f"{s2.group[0][1]}_summary", "summary", "agg", f"{t.name}_stats"]))
    if snake(n2) == snake(n1):
        raise Skip
    r2 = Rel(n2, snake(n2), out_columns(s2), sub=s2)
    q = Sel(r2)
    if chance(.4):
        q.star = True
    else:
        q.items = [(Col(c), c.name) for c in r2.cols]
        if len(q.items) > 2 and chance(.3):
            q.items.pop(rng.randrange(1, len(q.items)))
    numeric = [c for c in r2.cols if c.kind in ("int", "double") and c.name in dict((n, 1) for _, n in s2.aggs)]
    if numeric and chance(.5):
        c = choice(numeric)
        q.where = Cmp(Col(c), choice([">", ">=", "<"]), Lit(nice(c.lo, c.hi, c.kind)))
    add_order(q, st, feats, p=0.7, limit_p=0.3, unique=[n for _, n in s2.group])
    pad = "    " if st.multiline else ""
    sql = with_sql([(n1, s1.sql(st, pad)), (n2, s2.sql(st, pad))], q.sql(st), st)
    defs = [(snake(n1), s1.py_chain(ctx)), (snake(n2), s2.py_chain(ctx))]
    return sql, defs, q.py_chain(ctx), feats, [o[0] for o in q.order]


def fam_derived(st, ctx):
    t = choice(t for t in TABLES if any(c.kind in ("int", "double") for c in t.columns))
    rel = base_rel(t)
    feats = {"derived", "agg", "group"}
    inner = Sel(rel)
    inner.group = make_keys(rel, feats, rng.choices([1, 2], [.7, .3])[0])
    inner.aggs = make_aggs(rel, feats, rng.randint(1, 2), exclude=[n for _, n in inner.group])
    if chance(.4):
        inner.where = where([rel], feats, n=1)
    sub = Rel("sub", "sub", out_columns(inner), sub=inner, sub_sql_inline=True, py_inline=True,
              sql_alias=choice(["t", "sub", "agg", "x", "s"]))
    q = Sel(sub)
    if chance(.4):
        q.star = True
    else:
        q.items = [(Col(c), c.name) for c in sub.cols]
    numeric = [sub.col(n) for e, n in inner.aggs if e.kind in ("int", "double")]
    if not numeric:
        raise Skip
    c = choice(numeric)
    q.where = Cmp(Col(c), choice([">", ">=", "<"]), Lit(nice(c.lo, c.hi, c.kind)))
    add_order(q, st, feats, p=0.6, limit_p=0.3, unique=[n for _, n in inner.group])
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def fam_subquery(st, ctx):
    kind = wchoice([("in_parent", .2), ("exists_child", .22), ("not_exists_child", .15), ("not_in_parent", .15),
                    ("scalar", .2), ("correlated", .13)])
    feats = {"subquery", f"sub_{kind}"}
    if kind in ("scalar", "correlated"):
        return scalar_subquery(st, ctx, kind, feats)
    if kind in ("in_parent", "not_in_parent"):
        outer_t = choice(t for t in TABLES if fks(t))
        fk = choice(fks(outer_t))
        inner_t = TABLE_BY_NAME[fk.ref]
        oa, ia = aliases_for(st, [outer_t.name, inner_t.name])
        orel, irel = base_rel(outer_t, oa), base_rel(inner_t, ia)
        semi = Semi(irel, anti=kind == "not_in_parent", form="in", outer=orel.ref(fk), inner_col=inner_t.col("id"),
                    where=atom(base_rel(inner_t))[0], guard=kind == "not_in_parent" and fk.nullable)
    else:
        parent = choice(t for t in TABLES if children(t))
        child, fk = choice(children(parent))
        oa, ia = aliases_for(st, [parent.name, child.name])
        orel, irel = base_rel(parent, oa), base_rel(child, ia)
        anti = kind == "not_exists_child"
        form = "exists" if anti or chance(.6) else "in"
        semi = Semi(irel, anti=anti, form=form, outer=orel.ref(parent.col("id")), inner_col=fk,
                    where=atom(base_rel(child))[0] if chance(.6) else None)
        feats.add(f"sub_{form}")
    q = Sel(orel, semis=[semi])
    t = orel.table
    assert t is not None
    pool = [c for c in t.columns if c.kind not in ("array", "id")]
    q.items = [(orel.ref(t.col("id")), "id")] + [(orel.ref(c), c.name) for c in rng.sample(pool, rng.randint(1, 3))]
    if chance(.35):
        q.where = where([orel], feats, n=1)
    add_order(q, st, feats, p=0.4, limit_p=0.25, unique=["id"])
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


def scalar_subquery(st, ctx, kind, feats):
    k = st.k
    if kind == "scalar":
        t = choice(t for t in TABLES if any(c.kind in ("int", "double", "date") for c in t.columns))
        rel = base_rel(t)
        c = choice(c for c in t.columns if c.kind in ("int", "double", "date"))
        func = choice(["AVG", "MAX", "MIN"] if c.kind != "date" else ["MAX", "MIN"])
        inner = Sel(rel, aggs=[(Agg(func, Col(c)), "v")])
        if chance(.35):
            inner.where = where([rel], set(), n=1)
        inner_sql = f"{k('SELECT')} {Agg(func, Col(c)).sql(st)} {k('FROM')} {t.name}"
        if inner.where is not None:
            inner_sql += f" {k('WHERE')} {inner.where.sql(st)}"
        var = {"AVG": "avg", "MAX": "max", "MIN": "min"}[func] + f"_{c.name}"
        py_inner = f"df_{t.name}" + (f".filter({inner.where.py(ctx)})" if inner.where is not None else "")
        ctx.pre.append(f"{var} = {py_inner}.agg(F.{func.lower()}({pyq(c.name)})).first()[0]")
        op = ("=" if func in ("MAX", "MIN") and c.kind == "date" and chance(.6)
              else choice([">", ">="] if func != "MIN" else ["<", "<=", "="]))
        q = Sel(rel, where=Cmp(Col(c), op, PyVar(f"({inner_sql})", var)))
        pool = [x for x in t.columns if x.kind not in ("array", "id") and x.name != c.name]
        q.items = [(Col(t.col("id")), "id"), (Col(c), c.name)] + [(Col(x), x.name) for x in rng.sample(pool, rng.randint(0, 2))]
        add_order(q, st, feats, p=0.5, limit_p=0.25, unique=["id"])
        return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]
    # correlated scalar subquery -> window aggregate over the correlation key
    t = choice(t for t in TABLES if any(c.kind in ("cat", "fk") and not c.nullable for c in t.columns)
               and any(c.kind in ("int", "double") for c in t.columns))
    p = choice(c for c in t.columns if c.kind in ("cat", "fk") and not c.nullable)
    x = choice(c for c in t.columns if c.kind in ("int", "double"))
    func = choice(["AVG", "AVG", "MAX", "MIN"])
    a, b = ("a", "b") if chance(.5) else (t.name[0], t.name[0] + "2")
    helper = f"{func.lower()}_{x.name}_by_{p.name.replace('_id', '')}"
    op = {"AVG": choice([">", "<"]), "MAX": "=", "MIN": "="}[func]
    rel = base_rel(t)
    inner = Sel(rel, star=True, items=[(WinFn(func, func.lower(), [Col(x)], WinSpec([Col(p)]), kind="double"), helper)])
    sub = Rel(t.name, t.name, out_columns(inner), sub=inner, py_inline=True, table=t)
    pool = [c for c in t.columns if c.kind not in ("array", "id") and c.name not in (x.name, p.name)]
    extra = rng.sample(pool, rng.randint(0, 1))
    names = ["id", p.name, x.name] + [c.name for c in extra]
    outer = Sel(sub, items=[(Col(sub.col(n)), n) for n in names],
                where=Cmp(Col(sub.col(x.name)), op, Col(sub.col(helper))))
    add_order(outer, st, feats, p=0.4, limit_p=0)
    sel_list = ", ".join(f"{a}.{n}" for n in names)
    sql = (f"{k('SELECT')} {sel_list} {k('FROM')} {t.name} {a} {k('WHERE')} {a}.{x.name} {op} "
           f"({k('SELECT')} {k(func)}({b}.{x.name}) {k('FROM')} {t.name} {b} {k('WHERE')} {b}.{p.name} = {a}.{p.name})")
    if outer.order:
        sql += f" {k('ORDER BY')} " + ", ".join(f"{a}.{n}" + (f" {k('DESC')}" if d else "") for n, d, _ in outer.order)
    return sql, [], outer.py_chain(ctx), feats, [o[0] for o in outer.order]


def shared_col_pairs() -> list[tuple[Table, Table, list[str]]]:
    out = []
    for i, t1 in enumerate(TABLES):
        for t2 in TABLES[i + 1:]:
            shared = [c.name for c in t1.columns if c.kind in ("cat", "str")
                      and any(d.name == c.name and d.kind == c.kind for d in t2.columns)]
            if shared:
                out.append((t1, t2, shared))
    return out


MONEY_COLS = {"amount", "price", "unit_price", "cost", "revenue", "monthly_fee", "shipping_fee", "fee"}


def renamed_col_pairs() -> list[tuple[Table, Column, Table, Column]]:
    """Columns of two different tables with the same kind and value domain but different names."""
    out = []
    for t1 in TABLES:
        for t2 in TABLES:
            if t1.name == t2.name:
                continue
            for c1 in t1.columns:
                for c2 in t2.columns:
                    if c1.name == c2.name or c1.kind != c2.kind:
                        continue
                    if (c1.kind in ("cat", "str") and c1.pool and c1.pool == c2.pool) or c1.kind == "date" or (
                            c1.kind == "double" and c1.name in MONEY_COLS and c2.name in MONEY_COLS):
                        out.append((t1, c1, t2, c2))
    return out


def fam_setop(st, ctx):
    op = wchoice([("UNION ALL", .3), ("UNION", .4), ("INTERSECT", .15), ("EXCEPT", .15)])   # v4: more UNION (+ .distinct())
    feats = {"setop", "set_" + op.split()[0].lower() + ("_all" if "ALL" in op else "")}
    branch = wchoice([("same_table", .3), ("shared_names", .2), ("renamed", .5)])   # v5: more renamed columns
    if branch == "renamed":
        t1, c1, t2, c2 = choice(renamed_col_pairs())
        r1, r2 = base_rel(t1), base_rel(t2)
        mode = wchoice([("positional", .55), ("alias_right", .2), ("alias_both", .25)])
        if mode == "positional":        # SELECT city ... UNION SELECT location ...  -> .union() (by position)
            names = (c1.name, c2.name)
        elif mode == "alias_right":     # SELECT city ... UNION SELECT location AS city ...
            names = (c1.name, c1.name)
        else:                           # SELECT city AS label ... UNION SELECT location AS label ...
            common = {"date": "event_date", "double": "value"}.get(c1.kind, "label")
            names = (common, common)
        feats.add("set_" + mode)
        li2: list[tuple[E, str]] = [(Col(c1), names[0])]
        ri2: list[tuple[E, str]] = [(Col(c2), names[1])]
        if op.startswith("UNION") and chance(.5):
            lname = choice(["source", "kind", "record_type", "origin"])
            # v5: the literal column goes first or last
            at = 0 if chance(.5) else len(li2)
            li2.insert(at, (Lit(singular(t1.name)), lname))
            ri2.insert(at, (Lit(singular(t2.name)), lname))
            feats.add("literal_col")
            if at == 0:
                feats.add("literal_first")
        left, right = Sel(r1, items=li2), Sel(r2, items=ri2)
        if chance(.4):
            left.where = atom(r1)[0]
        if chance(.4):
            right.where = atom(r2)[0]
    elif branch == "same_table":
        t = choice(TABLES)
        r = base_rel(t)
        pool = [c for c in t.columns if c.kind in ("cat", "str", "int", "fk", "date")]
        cs = ([t.col("id")] if chance(.4) else []) + rng.sample(pool, rng.randint(1, 2))
        left = Sel(r, items=[(Col(c), c.name) for c in cs], where=atom(r)[0])
        right = Sel(base_rel(t), items=[(Col(c), c.name) for c in cs], where=atom(r)[0])
    else:
        t1, t2, shared = choice(shared_col_pairs())
        names = rng.sample(shared, min(len(shared), rng.randint(1, 2)))
        r1, r2 = base_rel(t1), base_rel(t2)
        li: list[tuple[E, str]] = [(Col(t1.col(n)), n) for n in names]
        ri: list[tuple[E, str]] = [(Col(t2.col(n)), n) for n in names]
        if op.startswith("UNION") and chance(.5):
            lname = choice(["source", "kind", "record_type", "origin"])
            at = 0 if chance(.5) else len(li)
            li.insert(at, (Lit(singular(t1.name)), lname))
            ri.insert(at, (Lit(singular(t2.name)), lname))
            feats.add("literal_col")
            if at == 0:
                feats.add("literal_first")
        left, right = Sel(r1, items=li), Sel(r2, items=ri)
        if chance(.4):
            left.where = atom(r1)[0]
        if chance(.4):
            right.where = atom(r2)[0]
    k = st.k
    nl = "\n" if st.multiline else " "
    sql = f"{left.sql(st)}{nl}{k(op)}{nl}{right.sql(st)}"
    head, steps = left.py_parts(ctx)
    rh, rs = right.py_parts(ctx)
    rline = rh + "".join(rs)
    arg = rline if len(rline) <= 70 else "\n" + "\n".join("    " + l for l in [rh] + rs) + "\n"
    # SQL set operations match columns by position: unionByName when the names line up, union() otherwise
    by_name = left.out_names() == right.out_names()
    method = {"UNION ALL": "unionByName" if by_name else "union", "UNION": "unionByName" if by_name else "union",
              "INTERSECT": "intersect", "EXCEPT": "subtract"}[op]
    steps.append(f".{method}({arg})")
    if op == "UNION":
        steps.append(".distinct()")
    order = []
    if chance(.4):
        n = choice(left.out_names())
        d = chance(.3)
        order = [(n, d)]
        sql += f"{nl}{k('ORDER BY')} {n}" + (f" {k('DESC')}" if d else "")
        steps.append(call("orderBy", [f"F.col({pyq(n)}).desc()" if d else pyq(n)]))
        feats.add("order")
    return sql, [], chain(head, steps), feats, [o[0] for o in order]


def fam_special(st, ctx):
    kind = wchoice([("pivot", .28), ("explode", .22), ("cross", .2), ("window_over_agg", .3)])   # v5: more DISTINCT cross joins
    feats = {kind}
    k = st.k
    nl = "\n" if st.multiline else " "
    if kind == "pivot":
        t = choice(t for t in TABLES if len([c for c in t.columns if c.kind == "cat"]) >= 2
                   and any(c.kind in ("int", "double") for c in t.columns))
        rel = base_rel(t)
        pc = choice(c for c in t.columns if c.kind == "cat" and len(c.pool) >= 3)
        key_e, key_n = make_key(rel, feats) if chance(.4) else (lambda c: (Col(c), c.name))(
            choice(c for c in t.columns if c.kind == "cat" and c.name != pc.name))
        if key_n == pc.name:
            raise Skip
        val = choice(c for c in t.columns if c.kind in ("int", "double"))
        vals = rng.sample([v for v in pc.pool if "'" not in v], rng.randint(2, min(4, len(pc.pool))))
        func = choice(["SUM", "AVG", "MAX", "COUNT"])
        w = where([rel], feats, n=1) if chance(.3) else None
        inner = f"{k('SELECT')} {key_e.sql(st) if isinstance(key_e, Col) else key_e.sql(st) + ' ' + k('AS') + ' ' + key_n}, {pc.name}, {val.name} {k('FROM')} {t.name}"
        if w is not None:
            inner += f" {k('WHERE')} {w.sql(st)}"
        in_list = ", ".join(f"'{v}'" for v in vals)
        sql = (f"{k('SELECT')} * {k('FROM')} ({inner}){nl}{k('PIVOT')} ({k(func)}({val.name}) {k('FOR')} {pc.name} "
               f"{k('IN')} ({in_list}))")
        steps = []
        if w is not None:
            steps.append(f".filter({w.py(ctx)})")
        key_py = pyq(key_n) if isinstance(key_e, Col) else f"{key_e.pyw(ctx)}.alias({pyq(key_n)})"
        steps += [f".groupBy({key_py})", f".pivot({pyq(pc.name)}, [{', '.join(pyq(v) for v in vals)}])",
                  f".agg(F.{func.lower()}({pyq(val.name)}))"]
        order = []
        if chance(.5):
            sql += f"{nl}{k('ORDER BY')} {key_n}"
            steps.append(f".orderBy({pyq(key_n)})")
            order = [key_n]
        return sql, [], chain(f"df_{t.name}", steps), feats, order
    if kind == "explode":
        t = TABLE_BY_NAME["products"]
        rel = base_rel(t)
        tag = choice(["tag", "tag_name", "label"])
        w = where([rel], feats, n=1) if chance(.4) else None
        wsql = f" {k('WHERE')} {w.sql(st)}" if w is not None else ""
        wpy = [f".filter({w.py(ctx)})"] if w is not None else []
        mode = choice(["select", "lateral", "group"])
        if mode in ("select", "lateral"):
            extra = rng.sample(["product_name", "category", "brand", "price"], rng.randint(1, 2))
            names = ["id"] + extra
            if mode == "select":
                sql = f"{k('SELECT')} {', '.join(names)}, {k('EXPLODE')}(tags) {k('AS')} {tag} {k('FROM')} products{wsql}"
            else:
                sql = (f"{k('SELECT')} {', '.join(names)}, {tag} {k('FROM')} products {k('LATERAL VIEW')} "
                       f"{k('EXPLODE')}(tags) t {k('AS')} {tag}{wsql}")
                feats.add("lateral_view")
            steps = wpy + [call("select", [pyq(n) for n in names] + [f'F.explode("tags").alias({pyq(tag)})'])]
            return sql, [], chain("df_products", steps), feats, []
        # (sql, pyspark, alias, columns the aggregate needs next to the exploded tag)
        agg = choice([("COUNT(*)", 'F.count("*")', "product_count", []),
                      ("ROUND(AVG(price), 2)", 'F.round(F.avg("price"), 2)', "avg_price", ["price"]),
                      ("SUM(stock_qty)", 'F.sum("stock_qty")', "total_stock", ["stock_qty"])])
        need = agg[3]
        sql_agg = agg[0] if st.upper else agg[0].lower()
        sql = (f"{k('SELECT')} {tag}, {sql_agg} {k('AS')} {agg[2]} {k('FROM')} products {k('LATERAL VIEW')} "
               f"{k('EXPLODE')}(tags) t {k('AS')} {tag}{wsql} {k('GROUP BY')} {tag}")
        steps = wpy + [call("select", [pyq(n) for n in need] + [f'F.explode("tags").alias({pyq(tag)})']),
                       f".groupBy({pyq(tag)})", f".agg({agg[1]}.alias({pyq(agg[2])}))"]
        order = []
        if chance(.6):
            sql += f" {k('ORDER BY')} {agg[2]} {k('DESC')}, {tag}"
            steps.append(f'.orderBy(F.col({pyq(agg[2])}).desc(), {pyq(tag)})')
            order = [agg[2], tag]
        feats |= {"lateral_view", "group"}
        return sql, [], chain("df_products", steps), feats, order
    if kind == "cross":
        t1, t2, _ = choice(shared_col_pairs())
        c1 = choice(c for c in t1.columns if c.kind == "cat")
        c2 = choice(c for c in t2.columns if c.kind == "cat" and c.name != c1.name)
        a1, a2 = aliases_for(st, ["x" + t1.name, "y" + t2.name]) if st.short_alias else ("a", "b")
        sql = (f"{k('SELECT')} {a1}.{c1.name}, {a2}.{c2.name} {k('FROM')} ({k('SELECT DISTINCT')} {c1.name} "
               f"{k('FROM')} {t1.name}) {a1} {k('CROSS JOIN')} ({k('SELECT DISTINCT')} {c2.name} {k('FROM')} {t2.name}) {a2}")
        steps = [f".select({pyq(c1.name)})", ".distinct()",
                 f".crossJoin(df_{t2.name}.select({pyq(c2.name)}).distinct())"]
        order = []
        if chance(.6):
            sql += f" {k('ORDER BY')} {c1.name}, {c2.name}"
            steps.append(f".orderBy({pyq(c1.name)}, {pyq(c2.name)})")
            order = [c1.name, c2.name]
        return sql, [], chain(f"df_{t1.name}", steps), feats, order
    # window over an aggregate: ranking groups / running totals / share of total
    t = choice(t for t in TABLES if any(c.kind in ("int", "double") and c.lo >= 0 for c in t.columns))
    rel = base_rel(t)
    feats |= {"agg", "group", "window"}
    q = Sel(rel)
    q.group = make_keys(rel, feats, 1)
    val = choice(c for c in t.columns if c.kind in ("int", "double") and c.lo >= 0)
    agg_e, agg_n = Agg("SUM", Col(val)), f"total_{val.name}"
    q.aggs = [(agg_e, agg_n)]
    if chance(.4):
        q.aggs.append((Agg("COUNT", kind="int"), "cnt"))
    if chance(.4):
        q.where = where([rel], feats, n=1)
    key_n = q.group[0][1]
    mode = choice(["rank", "running", "share"])
    out_c = Col(Column(agg_n, "double"))
    if mode == "rank":
        fn = choice([("RANK", "rank"), ("DENSE_RANK", "dense_rank")])
        nm = choice(["rnk", f"{val.name}_rank"])
        sql_item = f"{WinFn(fn[0], fn[1], [], WinSpec([], [(agg_e, True)])).sql(st)} {k('AS')} {nm}"
        py_e = WinFn(fn[0], fn[1], [], WinSpec([], [(out_c, True)]))
    elif mode == "running":
        nm = f"cumulative_{val.name}"
        key_c = Col(Column(key_n, "str"))
        sql_item = f"{WinFn('SUM', 'sum', [agg_e], WinSpec([], [(q.group[0][0], False)])).sql(st)} {k('AS')} {nm}"
        py_e = WinFn("SUM", "sum", [out_c], WinSpec([], [(key_c, False)]), kind="double")
        feats.add("window_running")
    else:
        nm = f"pct_of_total"
        sql_item = (f"{k('ROUND')}({agg_e.sql(st)} * 100.0 / {WinFn('SUM', 'sum', [agg_e], WinSpec()).sql(st)}, 2) "
                    f"{k('AS')} {nm}")
        py_e = Fn("ROUND", "round", [BinOp(BinOp(out_c, "*", Lit(100.0)), "/",
                                           WinFn("SUM", "sum", [out_c], WinSpec(), kind="double")), Raw(2)])
    q.post = [(sql_item, py_e, nm)]
    add_order(q, st, feats, p=.6, limit_p=.2, unique=[key_n])
    return q.sql(st), [], q.py_chain(ctx), feats, [o[0] for o in q.order]


FAMILIES = [(fam_select, 15), (fam_agg, 15), (fam_join, 12), (fam_join_agg, 8), (fam_window, 9), (fam_topn, 6),
            (fam_cte_agg_join, 7), (fam_cte_chain, 5), (fam_derived, 4), (fam_subquery, 9), (fam_setop, 7),
            (fam_special, 6)]

# v4: long multi-CTE pipelines, advanced Spark SQL and nested data (advanced.py)
advanced.bind(sys.modules[__name__])
FAMILIES += advanced.FAMILIES


def generate_one():
    while True:
        fam = rng.choices([f for f, _ in FAMILIES], [w for _, w in FAMILIES])[0]
        st, ctx = Style(rng), Ctx()
        try:
            sql, defs, result, feats, order_cols = fam(st, ctx)
        except (Skip, StopIteration, ValueError, IndexError):
            continue
        sql = sql.strip() + (";" if st.semicolon else "")
        code = render_code(ctx, defs, result)
        return fam.__name__[4:], sql, code, sorted(feats), order_cols


def normalize_sql(sql):
    return re.sub(r"\s+", " ", sql.lower()).strip().rstrip(";")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=15000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--out", default="raw_pairs")
    args = ap.parse_args()
    rng.seed(args.seed)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for f in out.glob("gen*.*"):
        f.unlink()
    seen, rows = set(), []
    while len(rows) < args.n:
        family, sql, code, feats, order_cols = generate_one()
        key = normalize_sql(sql)
        if key in seen:
            continue
        seen.add(key)
        pid = f"gen{len(rows) + 1:05d}"
        (out / f"{pid}.sql").write_text(sql + "\n", encoding="utf-8")
        (out / f"{pid}.py").write_text(code + "\n", encoding="utf-8")
        rows.append({"id": pid, "family": family, "features": feats, "order_cols": order_cols})
    with open(out / "meta_generated.jsonl", "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    fams = {}
    for r in rows:
        fams[r["family"]] = fams.get(r["family"], 0) + 1
    print(f"wrote {len(rows)} pairs to {out}/ ; by family: {dict(sorted(fams.items()))}")


if __name__ == "__main__":
    main()
