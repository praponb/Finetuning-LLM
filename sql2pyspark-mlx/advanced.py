"""v4 query families: long multi-CTE pipelines, advanced Spark SQL, and nested data.

The v1-v3 families in generate_dataset.py build one SELECT (sometimes with a CTE or two) from the object
model in sqlspec.py. Production SQL is longer and denser: 3-6 CTEs, several different windows,
sessionization with INTERVAL arithmetic, range / null-safe / LATERAL joins, QUALIFY, GROUPING SETS,
aggregate FILTER clauses, PIVOT / UNPIVOT, set operations with ALL, and nested data (structs, arrays of
structs, maps, JSON strings) handled with higher-order lambdas. The families here generate that SQL.

A query is a pipeline of *stages*. A stage turns one relation into the next and renders both a SELECT
(a CTE body, or the main query for the last stage) and the matching PySpark chain. Families choose stage
sequences; a one-stage pipeline is a short query focused on one feature.

Families return (sql, defs, result, feats, order_cols) like those in generate_dataset.py, and every pair
is still execution-checked by verify_pairs.py. Row-order-dependent windows (ROW_NUMBER, LAG, NTILE,
ROWS frames, FIRST/LAST_VALUE) always order by the relation's unique key as a tie-breaker, and are skipped
once a relation has no unique key, so both sides produce the same rows.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable

from schemas import (PROMO_CODES, TABLE_BY_NAME, Column, Table, extra_train_tables, make_rows, train_tables)
from sqlspec import (And, BinOp, Case, Cast, Cmp, Col, Ctx, E, Fn, IsNull, Lit, Or, Raw, Style, call, chain, fmt_num,
                     pyq, sel_py, sel_sql)

rng = random.Random(0)
G = None  # the generate_dataset module (atom / derive / nice / Skip ...), set by bind()


def bind(module):
    """Share generate_dataset's random generator and helpers."""
    global rng, G
    rng, G = module.rng, module


def skip():
    raise G.Skip


def choice(xs):
    return G.choice(xs)


def chance(p):
    return rng.random() < p


def wchoice(pairs):
    return G.wchoice(pairs)


PLAIN_KINDS = {"id", "fk", "int", "double", "cat", "str", "date", "ts", "bool", "array"}
KIND_OF_EXPR = {"int": "int", "double": "double", "str": "str", "date": "date", "ts": "ts", "bool": "bool",
                "array": "array"}


# ---------------------------------------------------------------------------
# Expressions not covered by sqlspec
# ---------------------------------------------------------------------------
@dataclass(eq=False)
class T(E):
    """An expression given as two render functions: SQL (style -> str) and PySpark (ctx -> str)."""
    s: Callable
    p: Callable
    kind: str = "double"
    wrap: bool = False      # PySpark text needs parentheses before method chaining

    def sql(self, st):
        return self.s(st)

    def py(self, ctx):
        return self.p(ctx)

    def pyw(self, ctx):
        return f"({self.p(ctx)})" if self.wrap else self.p(ctx)


def TS(sql: str, py: str, kind="double", wrap=False) -> T:
    """T with fixed texts (rendered eagerly, so later changes to loop variables can't leak in)."""
    return T(lambda st, _s=sql: _s, lambda ctx, _p=py: _p, kind=kind, wrap=wrap)


def C(name, kind="double", q=None):
    return Col(Column(name, kind if kind in G_KINDS else "double"), q)


G_KINDS = {"id", "fk", "int", "double", "cat", "str", "date", "ts", "bool", "array", "struct", "array_struct",
           "map", "json", "array_int", "array_double"}


def lit_sql(v, st):
    return Lit(v).sql(st)


def interval(n: int, unit: str) -> T:
    """INTERVAL '30' MINUTE / INTERVAL 30 MINUTES  ->  F.expr("INTERVAL 30 MINUTES")."""
    plural = unit + "S"
    form = rng.random()

    def s(st):
        if form < 0.5:
            return f"{st.k('INTERVAL')} '{n}' {st.k(unit)}"
        return f"{st.k('INTERVAL')} {n} {st.k(plural if n != 1 else unit)}"
    return T(s, lambda ctx: f'F.expr("INTERVAL {n} {plural if n != 1 else unit}")', kind="interval")


def ts_lit(v: str, date_only=False) -> T:
    form = rng.random()
    typ = "DATE" if date_only else "TIMESTAMP"

    def s(st):
        if form < 0.7:
            return f"{st.k(typ)} '{v}'"
        return f"{st.k('TO_DATE' if date_only else 'TO_TIMESTAMP')}('{v}')"

    def p(ctx):
        if form < 0.7:
            return f"F.lit({pyq(v)}).cast({pyq(typ.lower())})"
        return f"F.{'to_date' if date_only else 'to_timestamp'}(F.lit({pyq(v)}))"
    return T(s, p, kind="date" if date_only else "ts")


def null_safe_eq(a: E, b: E) -> T:
    return T(lambda st: f"{a.sql(st)} <=> {b.sql(st)}", lambda ctx: f"{a.pyw(ctx)}.eqNullSafe({b.py(ctx)})",
             kind="bool", wrap=False)


def try_cast(e: E, to: str) -> T:
    return T(lambda st: f"{st.k('TRY_CAST')}({e.sql(st)} {st.k('AS')} {st.k(to)})",
             lambda ctx: f"{e.pyw(ctx)}.try_cast({pyq(to.lower())})",
             kind={"DOUBLE": "double", "INT": "int", "BIGINT": "int", "DATE": "date"}.get(to, "double"))


def map_get(m: E, key: str) -> T:
    form = rng.random()

    def p(ctx):
        return f"{m.pyw(ctx)}[{pyq(key)}]" if form < 0.7 else f"{m.pyw(ctx)}.getItem({pyq(key)})"
    return T(lambda st: f"{m.sql(st)}['{key}']", p, kind="str")


def field_of(struct: E, name: str, kind="str") -> T:
    """struct.field  ->  F.col("struct.field") (or ["field"] on an expression)."""
    def p(ctx):
        if isinstance(struct, Col):
            return f"F.col({pyq(struct.ref() + '.' + name)})"
        return f"{struct.pyw(ctx)}[{pyq(name)}]"
    return T(lambda st: f"{struct.sql(st)}.{name}", p, kind=kind)


def rlike(e: E, pat: str) -> T:
    return T(lambda st: f"{e.sql(st)} {st.k('RLIKE')} '{pat}'", lambda ctx: f"{e.pyw(ctx)}.rlike({pyq(pat)})",
             kind="bool")


def bit_flag(e: E, bit: int) -> T:
    """(x & 4) != 0 -> (F.col("x").bitwiseAND(4) != 0)"""
    return T(lambda st: f"({e.sql(st)} & {bit}) != 0", lambda ctx: f"({e.pyw(ctx)}.bitwiseAND({bit}) != 0)",
             kind="bool")


def cast_int(e: E) -> T:
    return T(lambda st: f"{st.k('CAST')}({e.sql(st)} {st.k('AS')} {st.k('INT')})",
             lambda ctx: f"{e.pyw(ctx)}.cast(\"int\")", kind="int")


# ---------------------------------------------------------------------------
# Windows
# ---------------------------------------------------------------------------
def bound_sql(b):
    if isinstance(b, str):
        return {"UP": "UNBOUNDED PRECEDING", "CR": "CURRENT ROW", "UF": "UNBOUNDED FOLLOWING"}[b]
    if b == 0:
        return "CURRENT ROW"
    return f"{-b} PRECEDING" if b < 0 else f"{b} FOLLOWING"


def bound_py(b):
    if isinstance(b, str):
        return {"UP": "Window.unboundedPreceding", "CR": "Window.currentRow", "UF": "Window.unboundedFollowing"}[b]
    return "Window.currentRow" if b == 0 else str(b)


@dataclass(eq=False)
class WSpec:
    part: list = field(default_factory=list)      # [E]
    order: list = field(default_factory=list)     # [(E, desc, nulls)]   nulls: None / "first" / "last"
    frame: tuple | None = None                    # ("ROWS" | "RANGE", lo, hi)

    def sql(self, st):
        k, parts = st.k, []
        if self.part:
            parts.append(f"{k('PARTITION BY')} " + ", ".join(e.sql(st) for e in self.part))
        if self.order:
            items = []
            for e, d, nl in self.order:
                t = e.sql(st) + (" " + k("DESC") if d else (" " + k("ASC") if st.explicit_asc else ""))
                if nl:
                    t += " " + k(f"NULLS {nl.upper()}")
                items.append(t)
            parts.append(f"{k('ORDER BY')} " + ", ".join(items))
        if self.frame:
            unit, lo, hi = self.frame
            parts.append(k(f"{unit} BETWEEN {bound_sql(lo)} AND {bound_sql(hi)}"))
        return " ".join(parts)

    def py(self, ctx):
        s = "Window"
        if self.part:
            s += ".partitionBy(" + ", ".join(e.arg(ctx) for e in self.part) + ")"
        if self.order:
            s += ".orderBy(" + ", ".join(order_py(e, d, nl, ctx) for e, d, nl in self.order) + ")"
        if not self.part and not self.order:
            s = "Window.partitionBy()"
        if self.frame:
            unit, lo, hi = self.frame
            s += f".{'rowsBetween' if unit == 'ROWS' else 'rangeBetween'}({bound_py(lo)}, {bound_py(hi)})"
        return s

    def key(self):
        return self.py(Ctx())


def order_py(e, desc, nulls, ctx):
    if nulls:
        return f"{e.pyw(ctx)}.{'desc' if desc else 'asc'}_nulls_{nulls}()"
    if desc:
        return f"{e.pyw(ctx)}.desc()"
    return e.arg(ctx)


def window_var(ctx: Ctx, spec_py: str, name: str | None = None) -> str:
    ctx.uses_window = True
    if spec_py in ctx.windows:
        return ctx.windows[spec_py]
    if name is None or name in ctx.windows.values():
        name = "w" if not ctx.windows else f"w{len(ctx.windows) + 1}"
        while name in ctx.windows.values():
            name += "_"
    ctx.windows[spec_py] = name
    ctx.pre.append(f"{name} = {spec_py}")
    return name


@dataclass(eq=False)
class WFn(E):
    sql_name: str
    py_name: str
    args: list
    spec: WSpec
    kind: str = "int"
    py_args: list | None = None      # PySpark argument texts when they differ from args
    ignore_nulls: str | None = None  # "arg": LAST_VALUE(x, TRUE) / "kw": LAST_VALUE(x) IGNORE NULLS
    named: str | None = None         # SQL: OVER <named window>

    def sql(self, st):
        a = ", ".join(x.sql(st) for x in self.args)
        if self.ignore_nulls == "arg":
            a += ", " + st.k("TRUE")
        s = f"{st.k(self.sql_name)}({a})"
        if self.ignore_nulls == "kw":
            s += " " + st.k("IGNORE NULLS")
        return f"{s} {st.k('OVER')} " + (self.named if self.named else f"({self.spec.sql(st)})")

    def py(self, ctx):
        args = [x.arg(ctx) for x in self.args] if self.py_args is None else list(self.py_args)
        if self.ignore_nulls:
            args.append("ignorenulls=True")
        w = window_var(ctx, self.spec.py(ctx), self.named)
        return f"F.{self.py_name}({', '.join(args)}).over({w})"


# ---------------------------------------------------------------------------
# Relations, stage outputs, SQL layout
# ---------------------------------------------------------------------------
@dataclass
class R:
    name: str                 # SQL name (table or CTE)
    var: str                  # PySpark variable
    cols: list                # [Column]
    keys: list | None         # column names forming a unique row key (None: rows may repeat)
    base: Table | None = None

    def col(self, n):
        return next(c for c in self.cols if c.name == n)

    def has(self, n):
        return any(c.name == n for c in self.cols)

    def names(self):
        return [c.name for c in self.cols]

    def of(self, *kinds, nonnull=False, pooled=False):
        return [c for c in self.cols if c.kind in kinds and (not nonnull or not c.nullable)
                and (not pooled or (c.pool and all("'" not in v and "." not in v for v in c.pool)))]

    def plain_rel(self):
        """A sqlspec.Rel over the plain columns, for generate_dataset.atom / derive."""
        from sqlspec import Rel
        usable = [c for c in self.cols if c.kind in PLAIN_KINDS and (c.kind not in ("cat", "str") or c.pool)]
        return Rel(self.name, self.var, usable, table=self.base)


@dataclass
class Out:
    sql: str
    head: str
    steps: list
    cols: list
    keys: list | None
    hint: str = "step"
    comment: str | None = None
    order: list = field(default_factory=list)
    extra: list = field(default_factory=list)   # CTEs rendered before this one: [(name, var, sql, py, comment)]


@dataclass
class Gen:
    st: Style
    ctx: Ctx
    feats: set
    names: set = field(default_factory=set)     # CTE names in use


def fresh(name, taken):
    n, i = name, 2
    while n in taken:
        n, i = f"{name}_{i}", i + 1
    return n


def colspec(name, e: E, like: Column | None = None) -> Column:
    if like is not None:
        return Column(name, like.kind, pool=like.pool, lo=like.lo, hi=like.hi, ref=like.ref, nullable=True,
                      dtype=like.dtype)
    return Column(name, KIND_OF_EXPR.get(e.kind, "double"), lo=0, hi=100)


def sql_select(g: Gen, items, frm, joins=(), where=(), group=None, having=None, qualify=None, window=None,
               order=None, limit=None, distinct=False):
    st, k = g.st, g.st.k
    ml = st.multiline
    head = k("SELECT") + (" " + k("DISTINCT") if distinct else "")
    if ml and len(items) > 2:
        sel = head + "\n" + ",\n".join("    " + i for i in items)
    else:
        sel = head + " " + ", ".join(items)
    parts = [sel, f"{k('FROM')} {frm}"] + list(joins)
    if where:
        parts.append(f"{k('WHERE')} " + (f"\n  {k('AND')} " if ml else f" {k('AND')} ").join(where))
    if group:
        parts.append(f"{k('GROUP BY')} {group}")
    if having:
        parts.append(f"{k('HAVING')} {having}")
    if window:
        parts.append(f"{k('WINDOW')} " + ", ".join(window))
    if qualify:
        parts.append(f"{k('QUALIFY')} {qualify}")
    if order:
        parts.append(f"{k('ORDER BY')} {order}")
    if limit:
        parts.append(f"{k('LIMIT')} {limit}")
    return ("\n" if ml else " ").join(parts)


def paren(e_sql: str):
    return f"({e_sql})"


def pred_py(p: E, ctx):
    return p.py(ctx)


def order_sql(g, items):
    """items: [(sql_expr, desc, nulls)]"""
    k = g.st.k
    out = []
    for e, d, nl in items:
        t = e + (" " + k("DESC") if d else (" " + k("ASC") if g.st.explicit_asc else ""))
        if nl:
            t += " " + k(f"NULLS {nl.upper()}")
        out.append(t)
    return ", ".join(out)


def order_py_names(items):
    out = []
    for n, d, nl in items:
        if nl:
            out.append(f"F.col({pyq(n)}).{'desc' if d else 'asc'}_nulls_{nl}()")
        elif d:
            out.append(f"F.col({pyq(n)}).desc()")
        else:
            out.append(pyq(n))
    return out


# ---------------------------------------------------------------------------
# Data ranges (for literal thresholds and INTERVAL sizes)
# ---------------------------------------------------------------------------
_RANGES: dict = {}


def ts_info(table: Table | None, col: str):
    """(min, max, median gap in minutes) of a date/ts column in the seeded sample data."""
    if table is None or not any(c.name == col for c in table.columns):
        return None
    key = (table.name, col)
    if key not in _RANGES:
        i = [c.name for c in table.columns].index(col)
        vals = sorted(r[i] for r in make_rows(table) if r[i] is not None)
        vals = [v if isinstance(v, datetime) else datetime(v.year, v.month, v.day) for v in vals]
        gaps = sorted((b - a).total_seconds() / 60 for a, b in zip(vals, vals[1:]))
        _RANGES[key] = (vals[0], vals[-1], gaps[len(gaps) // 2] if gaps else 1440)
    return _RANGES[key]


def threshold(table, col, date_only=False):
    info = ts_info(table, col)
    if info is None:
        skip()
    lo, hi, _ = info
    t = lo + (hi - lo) * rng.uniform(0.05, 0.45)
    t = t.replace(minute=0, second=0, microsecond=0)
    if date_only or chance(0.5):
        return t.strftime("%Y-%m-%d"), True
    return t.strftime("%Y-%m-%d %H:%M:%S"), False


def gap_interval(table, col):
    """An INTERVAL near the typical gap between rows, so session splits are neither all nor none."""
    info = ts_info(table, col)
    med = info[2] if info else 1440
    if med < 90:
        n, unit = rng.choice([(15, "MINUTE"), (20, "MINUTE"), (30, "MINUTE"), (45, "MINUTE"), (1, "HOUR")])
    elif med < 24 * 60:
        n, unit = rng.choice([(1, "HOUR"), (2, "HOUR"), (4, "HOUR"), (6, "HOUR")])
    else:
        n, unit = rng.choice([(7, "DAY"), (14, "DAY"), (30, "DAY"), (60, "DAY"), (90, "DAY")])
    return n, unit


def range_seconds(table, col):
    info = ts_info(table, col)
    med = info[2] if info else 1440
    if med < 90:
        return rng.choice([900, 1800, 3600, 7200])
    if med < 24 * 60:
        return rng.choice([3600 * 6, 3600 * 12, 86400])
    return rng.choice([86400 * 7, 86400 * 30, 86400 * 90])


# ---------------------------------------------------------------------------
# Shared pickers
# ---------------------------------------------------------------------------
def part_candidates(r: R):
    cats = r.of("cat", pooled=True)
    return cats + r.of("fk")


def order_candidates(r: R):
    return r.of("ts", "date", "int", "double")


def tiebreak(r: R):
    return [(C(k, r.col(k).kind), False, None) for k in (r.keys or [])]


def as_arg(chain_text: str) -> str:
    """A (possibly multi-line, parenthesised) chain passed as a method argument, without doubled parentheses."""
    if chain_text.startswith("(\n") and chain_text.endswith("\n)"):
        return chain_text[1:-2] + "\n"
    return chain_text


def plural(n):
    if n.endswith("s"):
        return n
    if n.endswith("y") and n[-2:-1] not in "aeiou":
        return n[:-1] + "ies"
    return n + "s"


def num_cols(r: R, nonneg=False):
    return [c for c in r.of("int", "double") if (not nonneg or c.lo >= 0) and c.hi > c.lo]


def base_nums(r: R):
    """Numeric columns that come straight from the source table (safe for CORR / STDDEV: not constant)."""
    if r.base is None:
        return []
    names = {c.name for c in r.base.columns if c.kind in ("int", "double")}
    return [c for c in num_cols(r) if c.name in names]


# ---------------------------------------------------------------------------
# Stages (relational)
# ---------------------------------------------------------------------------
def stg_clean(g: Gen, cur: R, last: bool) -> Out:
    """Filter (timestamp literal / IS NOT NULL / predicate) + derived and COALESCE'd columns."""
    st, ctx = g.st, g.ctx
    rel = cur.plain_rel()
    conds: list[E] = []
    tcols = cur.of("ts", "date")
    if tcols and chance(0.6):
        c = choice(tcols)
        v, date_only = threshold(cur.base, c.name, c.kind == "date")
        conds.append(Cmp(C(c.name, c.kind), choice([">=", ">"]), ts_lit(v, date_only or c.kind == "date")))
        g.feats.add("ts_literal")
    nullable_keys = [c for c in part_candidates(cur) if c.nullable]
    if nullable_keys and chance(0.5):
        conds.append(IsNull(C(choice(nullable_keys).name, "int"), neg=True))
        g.feats.add("not_null_filter")
    if not conds or chance(0.35):
        p, f = G.atom(rel)
        conds.append(p)
        g.feats.add(f"where_{f}")
    derived = []
    taken = set(cur.names())
    for _ in range(rng.choice([1, 1, 2])):
        e, n = G.derive(rel, g.feats)
        n = fresh(n, taken)
        taken.add(n)
        derived.append((e, n))
    cats = [c for c in cur.of("cat", pooled=True) if c.nullable]
    if cats and chance(0.45):
        c = choice(cats)
        fill = choice(["UNKNOWN", "N/A", "OTHER", "DIRECT", "NONE"])
        n = fresh(choice([f"{c.name}_clean", f"resolved_{c.name}", f"{c.name}_filled"]), taken)
        taken.add(n)
        derived.append((Fn("COALESCE", "coalesce", [C(c.name, "cat"), Lit(fill)], kind="str"), n))
        g.feats.add("coalesce")
    explicit = chance(0.5) and len(cur.cols) > 4
    if explicit:
        must = set(cur.keys or []) | {c.name for c in cur.cols if c.kind not in PLAIN_KINDS}
        keep = [c for c in cur.cols if c.name in must or chance(0.75)]
        items_sql = [c.name for c in keep]
    else:
        keep = list(cur.cols)
        items_sql = ["*"]
    items_sql = items_sql + [sel_sql(e, n, st) for e, n in derived]
    where_sql = [(f"({p.sql(st)})" if isinstance(p, Or) else p.sql(st)) for p in conds]
    sql = sql_select(g, items_sql, cur.name, where=where_sql)
    pred = conds[0] if len(conds) == 1 else And(conds)
    steps = [f".filter({pred.py(ctx)})"]
    if explicit:
        steps.append(call("select", [pyq(c.name) for c in keep] + [sel_py(e, n, ctx) for e, n in derived]))
    elif chance(0.5):
        steps += [f".withColumn({pyq(n)}, {e.py(ctx)})" for e, n in derived]
    else:
        steps.append(call("select", ['"*"'] + [sel_py(e, n, ctx) for e, n in derived]))
    cols = keep + [colspec(n, e) for e, n in derived]
    keys = cur.keys if cur.keys and all(k in [c.name for c in cols] for k in cur.keys) else None
    g.feats.add("stage_clean")
    return Out(sql, cur.var, steps, cols, keys, hint=choice(["cleaned", "filtered", "base", "valid"]),
               comment=choice(["Filter the raw rows and derive helper columns", "Basic cleansing and defaults",
                               "Keep recent, valid rows"]))


def stg_dedup(g: Gen, cur: R, last: bool) -> Out:
    """ROW_NUMBER dedup via QUALIFY or a subquery (or RANK = 1 to keep ties)."""
    st, ctx = g.st, g.ctx
    parts = part_candidates(cur)
    orders = order_candidates(cur)
    if not parts or not orders or not cur.keys:
        skip()
    part = [C(c.name, c.kind) for c in rng.sample(parts, min(len(parts), rng.choice([1, 1, 2])))]
    oc = choice(orders)
    desc = chance(0.65)
    nulls = rng.choice([None, None, "last", "first"]) if oc.nullable else None
    fn = wchoice([(("ROW_NUMBER", "row_number"), 0.8), (("RANK", "rank"), 0.2)])
    order = [(C(oc.name, oc.kind), desc, nulls)] + (tiebreak(cur) if fn[0] == "ROW_NUMBER" else [])
    spec = WSpec(part, order)
    wf = WFn(fn[0], fn[1], [], spec)
    rn = fresh(choice(["rn", "row_num", "dedup_rank", "latest_rank", "rank_in_group"]), set(cur.names()))
    form = wchoice([("qualify_expr", 0.3), ("qualify_col", 0.3), ("subquery", 0.4)])
    g.feats |= {"stage_dedup", f"dedup_{form}", f"window_{fn[1]}"}
    if nulls:
        g.feats.add("nulls_order")
    if form.startswith("qualify"):
        g.feats.add("qualify")
    k = st.k
    if form == "qualify_expr":
        sql = sql_select(g, ["*"], cur.name, qualify=f"{wf.sql(st)} = 1")
        steps = [f".withColumn({pyq(rn)}, {wf.py(ctx)})", f".filter(F.col({pyq(rn)}) == 1)", f".drop({pyq(rn)})"]
        cols = list(cur.cols)
    else:
        if form == "qualify_col":
            sql = sql_select(g, ["*", f"{wf.sql(st)} {k('AS')} {rn}"], cur.name, qualify=f"{rn} = 1")
        else:
            inner = sql_select(g, ["*", f"{wf.sql(st)} {k('AS')} {rn}"], cur.name)
            if st.multiline:
                inner = "\n".join("    " + line for line in inner.split("\n"))
                frm = f"(\n{inner}\n) {choice(['ranked', 't', 'x'])}"
            else:
                frm = f"({inner}) {choice(['ranked', 't', 'x'])}"
            sql = sql_select(g, ["*"], frm, where=[f"{rn} = 1"])
        steps = [f".withColumn({pyq(rn)}, {wf.py(ctx)})", f".filter(F.col({pyq(rn)}) == 1)"]
        cols = list(cur.cols) + [Column(rn, "int", lo=1, hi=1, nullable=False)]
    return Out(sql, cur.var, steps, cols, cur.keys, hint=choice(["deduped", "latest", "ranked"]),
               comment=choice(["Keep one row per key (latest first)", "Deduplicate with ROW_NUMBER",
                               "QUALIFY-style window filter"]))


def make_window_item(g: Gen, cur: R, taken: set):
    """One window column: (WFn-ish expression, name, needs_total_order)."""
    parts = part_candidates(cur)
    orders = order_candidates(cur)
    nums = num_cols(cur)
    tss = cur.of("ts")
    if not orders:
        skip()
    part = [C(c.name, c.kind) for c in rng.sample(parts, min(len(parts), rng.choice([0, 1, 1, 1, 2])))] if parts else []
    has_keys = bool(cur.keys)
    opts = []
    oc = choice(orders)
    o = C(oc.name, oc.kind)
    if has_keys:
        tb = tiebreak(cur)
        if nums:
            v = choice(nums)
            off = rng.choice([1, 1, 2])
            fn = choice([("LAG", "lag"), ("LEAD", "lead")])
            dflt = chance(0.2)
            args = [C(v.name, v.kind), Raw(off)] + ([Raw(0)] if dflt else [])
            opts.append((WFn(fn[0], fn[1], args, WSpec(part, [(o, False, None)] + tb), kind=v.kind),
                                 f"{'prev' if fn[0] == 'LAG' else 'next'}_{v.name}", "window_lag"))
            opts.append((WFn("SUM", "sum", [C(v.name, v.kind)],
                                     WSpec(part, [(o, False, None)] + tb, ("ROWS", "UP", "CR"))),
                                 choice([f"running_{v.name}", f"cumulative_{v.name}"]), "window_running"))
            n = rng.choice([2, 3, 5, 6])
            hi = rng.choice([0, -1])
            opts.append((WFn("AVG", "avg", [C(v.name, v.kind)],
                                     WSpec(part, [(o, False, None)] + tb, ("ROWS", -n, hi))),
                                 f"{v.name}_avg_{n}{'_prior' if hi else ''}", "window_rows_frame"))
            opts.append((WFn("NTILE", "ntile", [Raw(rng.choice([3, 4, 5, 10]))],
                                     WSpec(part, [(C(v.name, v.kind), chance(0.5), None)] + tb)),
                                 f"{v.name}_bucket", "window_ntile"))
        cands = [c for c in cur.cols if c.kind in ("cat", "int", "double") and c.nullable]
        if cands:
            v2 = choice(cands)
            fnn = choice([("LAST_VALUE", "last"), ("FIRST_VALUE", "first")])
            ign = rng.choice(["arg", "kw"])
            opts.append((WFn(fnn[0], fnn[1], [C(v2.name, v2.kind)],
                                     WSpec(part, [(o, False, None)] + tb, ("ROWS", "UP", "CR")),
                                     kind=KIND_OF_EXPR.get(Col(v2).kind, "str"), ignore_nulls=ign),
                                 f"{'last' if fnn[0] == 'LAST_VALUE' else 'first'}_known_{v2.name}", "window_ignore_nulls"))
        opts.append((WFn("ROW_NUMBER", "row_number", [], WSpec(part, [(o, chance(0.5), None)] + tb)),
                             choice(["seq_no", "row_seq", "event_seq"]), "window_row_number"))
    if nums:
        v = choice(nums)
        nl = rng.choice([None, "last", "first"]) if v.nullable else None
        fr = choice([("DENSE_RANK", "dense_rank", "int"), ("RANK", "rank", "int"),
                     ("PERCENT_RANK", "percent_rank", "double"), ("CUME_DIST", "cume_dist", "double")])
        opts.append((WFn(fr[0], fr[1], [], WSpec(part, [(C(v.name, v.kind), chance(0.6), nl)]), kind=fr[2]),
                             f"{v.name}_{fr[1]}", f"window_{fr[1]}"))
        if part:
            agg = choice([("AVG", "avg"), ("MAX", "max"), ("MIN", "min"), ("SUM", "sum"), ("STDDEV_SAMP", "stddev_samp"),
                          ("COUNT", "count")])
            opts.append((WFn(agg[0], agg[1], [C(v.name, v.kind)], WSpec(part)),
                                 f"{part[0].name}_{agg[1]}_{v.name}", "window_partition_agg"))
            if v.lo > 0:
                opts.append((Fn("ROUND", "round", [BinOp(C(v.name, v.kind), "/",
                                                                WFn("SUM", "sum", [C(v.name, v.kind)], WSpec(part))),
                                                          Raw(4)]),
                                     f"{v.name}_share", "window_share"))
            bn = base_nums(cur)
            others = [c for c in bn if c.name != v.name]
            if others and v in bn:
                y = choice(others)
                opts.append((WFn("CORR", "corr", [C(v.name, v.kind), C(y.name, y.kind)], WSpec(part),
                                         kind="double"), f"corr_{v.name}_{y.name}", "window_corr"))
        if tss:
            tcol = choice(tss)
            secs = range_seconds(cur.base, tcol.name)
            ep = Cast(C(tcol.name, "ts"), "BIGINT")
            fa = choice([("SUM", "sum"), ("AVG", "avg"), ("MAX", "max"), ("COUNT", "count")])
            label = {900: "15m", 1800: "30m", 3600: "1h", 7200: "2h", 21600: "6h", 43200: "12h", 86400: "1d",
                     604800: "7d", 2592000: "30d", 7776000: "90d"}.get(secs, f"{secs}s")
            opts.append((WFn(fa[0], fa[1], [C(v.name, v.kind)],
                                     WSpec(part, [(ep, False, None)], ("RANGE", -secs, "CR"))),
                                 f"{v.name}_{fa[1]}_{label}", "window_range_frame"))
    if not opts:
        skip()
    e, n, f = choice(opts)
    n = fresh(n, taken)
    taken.add(n)
    g.feats.add(f)
    return e, n


def win_col(n, e, cur: R) -> Column:
    """Column descriptor for a window output, with a realistic value range for later thresholds."""
    from sqlspec import find
    kind = KIND_OF_EXPR.get(e.kind, "double")
    w = e if isinstance(e, WFn) else next(iter(find(e, WFn)), None)
    lo, hi = 0, 100
    if isinstance(e, Fn) and any(True for _ in find(e, BinOp)):
        lo, hi = 0, 1                                   # share of partition total
    elif w is not None:
        src = next((cur.col(a.name) for a in w.args if isinstance(a, Col) and cur.has(a.name)), None)
        if w.py_name in ("row_number", "rank", "dense_rank", "ntile"):
            lo, hi = 1, 10
        elif w.py_name in ("percent_rank", "cume_dist"):
            lo, hi = 0, 1
        elif w.py_name == "corr":
            lo, hi = -1, 1
        elif w.py_name == "count":
            lo, hi = 1, 20
        elif src is not None:
            lo, hi = src.lo, (src.hi * 5 if w.py_name == "sum" else src.hi)
    return Column(n, kind, lo=lo, hi=hi)


def stg_windows(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    taken = set(cur.names())
    items = [make_window_item(g, cur, taken) for _ in range(rng.choice([2, 2, 3, 3, 4, 5]))]
    # named WINDOW clause when two items share a spec
    wins = [e for e, _ in items if isinstance(e, WFn)]
    window_clause = []
    specs = {}
    for w in wins:
        specs.setdefault(w.spec.sql(st), []).append(w)
    if chance(0.5):
        for i, (sq, ws) in enumerate(specs.items()):
            if len(ws) >= 2 or chance(0.3):
                nm = choice(["w_part", "w_ordered", "w_running", "w_recent", "w_grp", "w_hist"])
                nm = fresh(nm, {x.split(" ")[0] for x in window_clause})
                for w in ws:
                    w.named = nm
                window_clause.append(f"{nm} {st.k('AS')} ({sq})")
                g.feats.add("named_window")
    explicit = chance(0.35)
    items_sql = ([c.name for c in cur.cols] if explicit else ["*"]) + [sel_sql(e, n, st) for e, n in items]
    sql = sql_select(g, items_sql, cur.name, window=window_clause or None)
    if chance(0.5):
        steps = [f".withColumn({pyq(n)}, {e.py(ctx)})" for e, n in items]
    else:
        steps = [call("select", ['"*"'] + [sel_py(e, n, ctx) for e, n in items])]
    cols = list(cur.cols) + [win_col(n, e, cur) for e, n in items]
    g.feats.add("stage_windows")
    return Out(sql, cur.var, steps, cols, cur.keys, hint=choice(["enriched", "with_windows", "metrics", "scored"]),
               comment=choice(["Window metrics over several different partitions", "Running and moving aggregates",
                               "Ranking and gap features"]))


def stg_session(g: Gen, cur: R, last: bool) -> Out:
    """Gaps-and-islands: flag rows after a gap > INTERVAL, then a running SUM of the flag."""
    st, ctx = g.st, g.ctx
    tss = cur.of("ts")
    parts = part_candidates(cur)
    if not tss or not parts or not cur.keys:
        skip()
    t = choice(tss)
    part = [C(choice(parts).name, "int")]
    tb = tiebreak(cur)
    spec = WSpec(part, [(C(t.name, "ts"), False, None)] + tb)
    lag = WFn("LAG", "lag", [C(t.name, "ts")], spec, kind="ts")
    n, unit = gap_interval(cur.base, t.name)
    iv = interval(n, unit)
    taken = set(cur.names())
    flag = fresh(choice(["is_new_session", "is_new_burst", "new_group_flag", "session_start_flag"]), taken)
    taken.add(flag)
    form = wchoice([("null_or_gap", 0.45), ("diff_gt", 0.35), ("prev_col", 0.2)])
    k = st.k
    prev_name = None
    if form == "null_or_gap":
        cond_sql = lambda: (f"{lag.sql(st)} {k('IS NULL')}\n             {k('OR')} {t.name} > {lag.sql(st)} + {iv.sql(st)}"
                            if st.multiline else f"{lag.sql(st)} {k('IS NULL')} {k('OR')} {t.name} > {lag.sql(st)} + {iv.sql(st)}")
        case_sql = f"{k('CASE')} {k('WHEN')} {cond_sql()} {k('THEN')} 1 {k('ELSE')} 0 {k('END')}"
        lag_py = lag.py(ctx)
        case_py = (f"F.when({lag_py}.isNull() | (F.col({pyq(t.name)}) > {lag_py} + {iv.py(ctx)}), 1).otherwise(0)")
    elif form == "diff_gt":
        case_sql = f"{k('CASE')} {k('WHEN')} {t.name} - {lag.sql(st)} > {iv.sql(st)} {k('THEN')} 1 {k('ELSE')} 0 {k('END')}"
        case_py = f"F.when(F.col({pyq(t.name)}) - {lag.py(ctx)} > {iv.py(ctx)}, 1).otherwise(0)"
    else:
        prev_name = fresh(f"prev_{t.name}", taken)
        taken.add(prev_name)
        case_sql = None
        case_py = None
    sess = fresh(choice(["session_no", "session_id", "burst_id", "island_id"]), taken)
    run_spec = WSpec(part, [(C(t.name, "ts"), False, None)] + tb, ("ROWS", "UP", "CR"))
    run = WFn("SUM", "sum", [C(flag, "int")], run_spec)
    g.feats |= {"stage_session", "sessionize", "interval", f"session_{form}"}
    fname = fresh(choice(["flagged", "gaps", "with_gaps"]), g.names)
    g.names.add(fname)
    if form == "prev_col":
        # two steps: prev timestamp column, then the flag computed from it
        flag_sql = sql_select(g, ["*", f"{lag.sql(st)} {k('AS')} {prev_name}"], cur.name)
        flag_py = chain(cur.var, [f".withColumn({pyq(prev_name)}, {lag.py(ctx)})"])
        pname = fresh(choice(["with_prev", "prev_events"]), g.names)
        g.names.add(pname)
        case_sql = (f"{k('CASE')} {k('WHEN')} {prev_name} {k('IS NULL')} {k('OR')} {t.name} > {prev_name} + {iv.sql(st)} "
                    f"{k('THEN')} 1 {k('ELSE')} 0 {k('END')}")
        case_py = (f"F.when(F.col({pyq(prev_name)}).isNull() | (F.col({pyq(t.name)}) > F.col({pyq(prev_name)}) + "
                   f"{iv.py(ctx)}), 1).otherwise(0)")
        flag2_sql = sql_select(g, ["*", f"{case_sql} {k('AS')} {flag}"], pname)
        flag2_py = chain(pname, [f".withColumn({pyq(flag)}, {case_py})"])
        extra = [(pname, pname, flag_sql, flag_py, "Previous event time per key"),
                 (fname, fname, flag2_sql, flag2_py, "Flag rows that start a new session")]
        base_cols = list(cur.cols) + [Column(prev_name, "ts"), Column(flag, "int", lo=0, hi=1)]
    else:
        flag_sql = sql_select(g, ["*", f"{case_sql} {k('AS')} {flag}"], cur.name)
        flag_py = chain(cur.var, [f".withColumn({pyq(flag)}, {case_py})"])
        extra = [(fname, fname, flag_sql, flag_py, "Flag rows that start a new session (gap > interval)")]
        base_cols = list(cur.cols) + [Column(flag, "int", lo=0, hi=1)]
    sql = sql_select(g, ["*", f"{run.sql(st)} {k('AS')} {sess}"], fname)
    steps = [f".withColumn({pyq(sess)}, {run.py(ctx)})"]
    cols = base_cols + [Column(sess, "int", lo=0, hi=10, nullable=False)]
    return Out(sql, fname, steps, cols, cur.keys, hint=choice(["sessionized", "sessions", "islands"]),
               comment="Running sum of the flag = session number", extra=extra)


def fk_parents(r: R):
    out = []
    for c in r.of("fk"):
        if c.ref in TABLE_BY_NAME and not TABLE_BY_NAME[c.ref].holdout:
            out.append((c, TABLE_BY_NAME[c.ref]))
    return out


def stg_join(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    k = st.k
    opts = []
    if fk_parents(cur):
        opts.append(("fk", 0.45))
    if cur.has("channel") and cur.of("date", "ts"):
        opts.append(("promo_range", 0.35))
    if cur.has("customer_id") and cur.of("date", "ts") and cur.keys:
        opts.append(("subscription_range", 0.2))
    if not opts:
        skip()
    kind = wchoice(opts)
    sa = choice(["s", "src", "e", "a"])
    taken = set(cur.names())
    keys = cur.keys
    if kind == "fk":
        fkc, parent = choice(fk_parents(cur))
        pa = choice(["p", "d", "b", parent.name[0] if parent.name[0] != sa else "p2"])
        if pa == sa:
            pa = "p"
        pcols = [c for c in parent.columns if c.kind in ("cat", "str", "int", "double", "date") and c.name != "id"]
        if not pcols:
            skip()
        chosen = rng.sample(pcols, min(len(pcols), rng.randint(1, 3)))
        how = choice(["left", "inner", "left"])
        cond_sql = f"{sa}.{fkc.name} = {pa}.id"
        cond_py = f"F.col({pyq(sa + '.' + fkc.name)}) == F.col({pyq(pa + '.id')})"
        outs = []
        for c in chosen:
            n = c.name if c.name not in taken else fresh(f"{G.singular(parent.name)}_{c.name}", taken)
            taken.add(n)
            outs.append((c, n))
        join_sql = f"{k('LEFT JOIN') if how == 'left' else k(st.inner_kw)} {parent.name} {pa}\n    {k('ON')} {cond_sql}" \
            if st.multiline else f"{k('LEFT JOIN') if how == 'left' else k(st.inner_kw)} {parent.name} {pa} {k('ON')} {cond_sql}"
        items_sql = [f"{sa}.*"] + [f"{pa}.{c.name}" + ("" if n == c.name else f" {k('AS')} {n}") for c, n in outs]
        sql = sql_select(g, items_sql, f"{cur.name} {sa}", joins=[join_sql])
        steps = [f".alias({pyq(sa)})",
                 f".join(df_{parent.name}.alias({pyq(pa)}), {cond_py}, {pyq(how)})",
                 call("select", [pyq(sa + ".*")] + [f"F.col({pyq(pa + '.' + c.name)})" + (f".alias({pyq(n)})" if n != c.name else "")
                                                   for c, n in outs])]
        cols = list(cur.cols) + [Column(n, c.kind, pool=c.pool, lo=c.lo, hi=c.hi, nullable=True) for c, n in outs]
        g.feats |= {"stage_join", "join_fk", f"join_{how}"}
        hint, comment = choice(["joined", "with_dims", "enriched"]), f"Attach {parent.name} attributes"
    else:
        tcol = choice(cur.of("date", "ts"))
        if kind == "promo_range":
            other, oa, ocols = "promotions", "pr", [("promo_code", "promo_code", "cat"), ("discount_pct", "discount_pct", "double")]
            s_d, e_d, oid = "starts_on", "ends_on", "promo_id"
            key_form = wchoice([("eq", 0.4), ("null_safe", 0.3), ("coalesce", 0.3)])
            if key_form == "eq":
                k_sql, k_py = f"{sa}.channel = {oa}.channel", f"F.col({pyq(sa + '.channel')}) == F.col({pyq(oa + '.channel')})"
            elif key_form == "null_safe":
                k_sql, k_py = f"{sa}.channel <=> {oa}.channel", f"F.col({pyq(sa + '.channel')}).eqNullSafe(F.col({pyq(oa + '.channel')}))"
                g.feats.add("null_safe_join")
            else:
                dflt = choice(["web", "store"])
                k_sql = f"{k('COALESCE')}({sa}.channel, '{dflt}') = {oa}.channel"
                k_py = f"F.coalesce(F.col({pyq(sa + '.channel')}), F.lit({pyq(dflt)})) == F.col({pyq(oa + '.channel')})"
                g.feats.add("coalesce_join_key")
        else:
            other, oa, ocols = "subscriptions", "sub", [("plan", "plan", "cat"), ("monthly_fee", "monthly_fee", "double")]
            s_d, e_d, oid = "start_date", "end_date", "subscription_id"
            k_sql = f"{sa}.customer_id = {oa}.customer_id"
            k_py = f"F.col({pyq(sa + '.customer_id')}) == F.col({pyq(oa + '.customer_id')})"
        rng_form = wchoice([("interval", 0.45), ("between", 0.3), ("date_add", 0.25)])
        tq = f"{sa}.{tcol.name}"
        if rng_form == "interval":
            nd = rng.choice([7, 14, 30])
            iv = interval(nd, "DAY")
            r_sql = [f"{tq} >= {oa}.{s_d}", f"{tq} < {oa}.{e_d} + {iv.sql(st)}"]
            r_py = [f"(F.col({pyq(tq)}) >= F.col({pyq(oa + '.' + s_d)}))",
                    f"(F.col({pyq(tq)}) < F.col({pyq(oa + '.' + e_d)}) + {iv.py(ctx)})"]
            g.feats.add("interval")
        elif rng_form == "between":
            r_sql = [f"{tq} {k('BETWEEN')} {oa}.{s_d} {k('AND')} {oa}.{e_d}"]
            r_py = [f"F.col({pyq(tq)}).between(F.col({pyq(oa + '.' + s_d)}), F.col({pyq(oa + '.' + e_d)}))"]
        else:
            nd = rng.choice([3, 7, 10])
            r_sql = [f"{tq} >= {oa}.{s_d}", f"{tq} <= {k('DATE_ADD')}({oa}.{e_d}, {nd})"]
            r_py = [f"(F.col({pyq(tq)}) >= F.col({pyq(oa + '.' + s_d)}))",
                    f"(F.col({pyq(tq)}) <= F.date_add(F.col({pyq(oa + '.' + e_d)}), {nd}))"]
        how = choice(["left", "left", "inner"])
        cond_parts_sql = [k_sql] + r_sql
        if st.multiline:
            join_sql = (f"{k('LEFT JOIN') if how == 'left' else k(st.inner_kw)} {other} {oa}\n    {k('ON')} "
                        + f"\n   {k('AND')} ".join(cond_parts_sql))
        else:
            join_sql = f"{k('LEFT JOIN') if how == 'left' else k(st.inner_kw)} {other} {oa} {k('ON')} " + f" {k('AND')} ".join(cond_parts_sql)
        cond_py = " & ".join([f"({k_py})"] + r_py)
        outs = []
        for src, n, kd in ocols:
            n2 = n if n not in taken else fresh(f"{other[:-1]}_{n}", taken)
            taken.add(n2)
            outs.append((src, n2, kd))
        oid = fresh(oid, taken)
        taken.add(oid)
        items_sql = [f"{sa}.*", f"{oa}.id {k('AS')} {oid}"] + [f"{oa}.{src}" + ("" if n == src else f" {k('AS')} {n}")
                                                              for src, n, _ in outs]
        sql = sql_select(g, items_sql, f"{cur.name} {sa}", joins=[join_sql])
        steps = [f".alias({pyq(sa)})", f".join(df_{other}.alias({pyq(oa)}), {cond_py}, {pyq(how)})",
                 call("select", [pyq(sa + ".*"), f"F.col({pyq(oa + '.id')}).alias({pyq(oid)})"]
                      + [f"F.col({pyq(oa + '.' + src)})" + (f".alias({pyq(n)})" if n != src else "") for src, n, _ in outs])]
        ref = TABLE_BY_NAME[other]
        cols = list(cur.cols) + [Column(oid, "int", lo=1, hi=12)] + [
            Column(n, kd, pool=ref.col(src).pool, lo=ref.col(src).lo, hi=ref.col(src).hi) for src, n, kd in outs]
        keys = (cur.keys + [oid]) if cur.keys else None
        g.feats |= {"stage_join", "range_join", f"join_{how}", f"range_{rng_form}"}
        hint, comment = choice(["attributed", "matched", "with_window"]), "Non-equi join on a validity window"
    return Out(sql, cur.var, steps, cols, keys, hint=hint, comment=comment)


def children_of(table: Table | None):
    if table is None:
        return []
    out = []
    for t in train_tables():
        for c in t.columns:
            if c.kind == "fk" and c.ref == table.name and t.name != table.name:
                out.append((t, c))
    return out


def stg_anti(g: Gen, cur: R, last: bool) -> Out:
    """[NOT] EXISTS with an extra correlated condition -> left_anti / left_semi join with a compound condition."""
    st, ctx = g.st, g.ctx
    k = st.k
    if not cur.has("id") or cur.base is None:
        skip()
    kids = [(t, c) for t, c in children_of(cur.base)]
    if not kids:
        skip()
    child, fkc = choice(kids)
    sa, ca = "s", choice(["c", "x", "ch"])
    conds_sql = [f"{ca}.{fkc.name} = {sa}.id"]
    conds_py = [f"(F.col({pyq(ca + '.' + fkc.name)}) == F.col({pyq(sa + '.id')}))"]
    cd = [c for c in child.columns if c.kind in ("date", "ts")]
    sd = cur.of("date", "ts")
    if cd and sd and chance(0.6):
        a, b = choice(cd), choice(sd)
        op = choice(["<=", ">=", "<"])
        conds_sql.append(f"{ca}.{a.name} {op} {sa}.{b.name}")
        conds_py.append(f"(F.col({pyq(ca + '.' + a.name)}) {op} F.col({pyq(sa + '.' + b.name)}))")
        g.feats.add("correlated_nonequi")
    else:
        from sqlspec import Rel
        p, f = G.atom(Rel(child.name, f"df_{child.name}", [c for c in child.columns if c.kind in PLAIN_KINDS
                                                            and (c.kind not in ("cat", "str") or c.pool)], table=child))
        conds_sql.append(_qualify_sql(p, ca, st))
        conds_py.append(f"({_qualify_py(p, ca, ctx)})")
    anti = chance(0.65)
    sub = f"{k('SELECT')} 1 {k('FROM')} {child.name} {ca} {k('WHERE')} " + f" {k('AND')} ".join(conds_sql)
    if st.multiline:
        sub = (f"{k('SELECT')} 1\n        {k('FROM')} {child.name} {ca}\n        {k('WHERE')} "
               + f"\n          {k('AND')} ".join(conds_sql))
        pred = f"{k('NOT EXISTS') if anti else k('EXISTS')} (\n        {sub}\n    )"
    else:
        pred = f"{k('NOT EXISTS') if anti else k('EXISTS')} ({sub})"
    sql = sql_select(g, [f"{sa}.*"], f"{cur.name} {sa}", where=[pred])
    steps = [f".alias({pyq(sa)})",
             f".join(df_{child.name}.alias({pyq(ca)}), {' & '.join(conds_py)}, {pyq('left_anti' if anti else 'left_semi')})"]
    g.feats |= {"stage_anti", "not_exists" if anti else "exists", "semi_anti_join"}
    return Out(sql, cur.var, steps, list(cur.cols), cur.keys, hint=choice(["kept", "eligible", "not_flagged"]),
               comment=f"{'Anti' if anti else 'Semi'}-join via a correlated subquery")


def _qualify_sql(p: E, alias: str, st):
    from sqlspec import requal
    return requal(p, alias).sql(st)


def _qualify_py(p: E, alias: str, ctx):
    from sqlspec import requal
    return requal(p, alias).py(ctx)


AGG_NAMES = {"sum": "total", "avg": "avg", "max": "max", "min": "min", "count": "cnt"}


def make_agg_item(g: Gen, cur: R, taken: set):
    st = g.st
    k = st.k
    nums = num_cols(cur)
    cats = cur.of("cat", pooled=True)
    ints = [c for c in cur.of("int") if c.lo >= 0 and cur.base is not None
            and any(b.name == c.name and b.kind == "int" for b in cur.base.columns)]
    opts = [(TS(f"{k('COUNT')}(*)", 'F.count("*")', kind="int"),
                     choice(["cnt", "row_count", "event_count", "num_rows"]), "agg_count")]
    keyish = cur.of("fk", "cat", "id")
    if keyish:
        c = choice(keyish)
        opts.append((TS(f"{k('COUNT')}({k('DISTINCT')} {c.name})",
                               f"F.countDistinct({pyq(c.name)})", kind="int"),
                             f"distinct_{plural(c.name)}", "agg_count_distinct"))
    if nums:
        v = choice(nums)
        fn = choice(["SUM", "AVG", "MAX", "MIN"])
        opts.append((TS(f"{k(fn)}({v.name})", f"F.{fn.lower()}({pyq(v.name)})"),
                             f"{AGG_NAMES[fn.lower()]}_{v.name}", "agg_basic"))
        opts.append((TS(f"{k('ROUND')}({k('AVG')}({v.name}), 2)",
                               f"F.round(F.avg({pyq(v.name)}), 2)"), f"avg_{v.name}", "agg_basic"))
        if v in base_nums(cur):
            opts.append((TS(f"{k('STDDEV_SAMP')}({v.name})", f"F.stddev_samp({pyq(v.name)})"),
                                 f"{v.name}_stddev", "agg_stddev"))
        pct = choice(["0.5", "0.9", "0.95"])
        fnp = choice(["PERCENTILE_APPROX", "APPROX_PERCENTILE"])
        opts.append((TS(f"{k(fnp)}({v.name}, {pct})",
                               f"F.percentile_approx({pyq(v.name)}, {pct})"),
                             f"{v.name}_p{pct[2:].ljust(2, '0')}", "agg_percentile"))
        opts.append((TS(f"{k(fnp)}({v.name}, {k('ARRAY')}(0.25, 0.5, 0.75))",
                               f"F.percentile_approx({pyq(v.name)}, [0.25, 0.5, 0.75])", kind="array"),
                             f"{v.name}_quartiles", "agg_percentile_array"))
        others = [c for c in base_nums(cur) if c.name != v.name]
        if others and v in base_nums(cur):
            y = choice(others)
            opts.append((TS(f"{k('CORR')}({v.name}, {y.name})",
                                   f"F.corr({pyq(v.name)}, {pyq(y.name)})"),
                                 f"corr_{v.name}_{y.name}", "agg_corr"))
        if cats:
            c = choice(cats)
            val = G.pool_val(c)
            vn = G.ident(val)
            opts.append((TS(f"{k('SUM')}({k('CASE')} {k('WHEN')} {c.name} = '{val}' {k('THEN')} {v.name} {k('ELSE')} 0 {k('END')})",
                                   f"F.sum(F.when(F.col({pyq(c.name)}) == {pyq(val)}, F.col({pyq(v.name)})).otherwise(0))"),
                                 f"{vn}_{v.name}", "agg_conditional"))
            fa = choice(["SUM", "AVG", "MAX"])
            opts.append((TS(f"{k(fa)}({v.name}) {k('FILTER')} ({k('WHERE')} {c.name} = '{val}')",
                                   f"F.{fa.lower()}(F.when(F.col({pyq(c.name)}) == {pyq(val)}, F.col({pyq(v.name)})))"),
                                 f"{vn}_{AGG_NAMES[fa.lower()]}_{v.name}", "agg_filter_clause"))
            opts.append((TS(f"{k('ROUND')}({k('SUM')}({v.name}) / {k('NULLIF')}({k('COUNT')}({k('CASE')} {k('WHEN')} {c.name} = '{val}' {k('THEN')} 1 {k('END')}), 0), 2)",
                                   f"F.round(F.sum({pyq(v.name)}) / F.nullif(F.count(F.when(F.col({pyq(c.name)}) == {pyq(val)}, 1)), F.lit(0)), 2)"),
                                 f"{v.name}_per_{vn}", "agg_nullif_ratio"))
    if cats:
        c = choice(cats)
        val = G.pool_val(c)
        vn = G.ident(val)
        opts.append((TS(f"{k('COUNT')}({k('CASE')} {k('WHEN')} {c.name} = '{val}' {k('THEN')} 1 {k('END')})",
                               f"F.count(F.when(F.col({pyq(c.name)}) == {pyq(val)}, 1))", kind="int"),
                             f"{vn}_count", "agg_conditional"))
        opts.append((TS(f"{k('COUNT')}(*) {k('FILTER')} ({k('WHERE')} {c.name} = '{val}')",
                               f"F.count(F.when(F.col({pyq(c.name)}) == {pyq(val)}, 1))", kind="int"),
                             f"{vn}_rows", "agg_filter_clause"))
        opts.append((TS(f"{k('COUNT_IF')}({c.name} = '{val}')",
                               f"F.count_if(F.col({pyq(c.name)}) == {pyq(val)})", kind="int"),
                             f"{vn}_events", "agg_count_if"))
        opts.append((TS(f"{k('SORT_ARRAY')}({k('COLLECT_SET')}({c.name}))",
                               f"F.sort_array(F.collect_set({pyq(c.name)}))", kind="array"),
                             f"{c.name}_list", "agg_collect"))
        opts.append((TS(f"{k('SIZE')}({k('COLLECT_SET')}({c.name}))",
                               f"F.size(F.collect_set({pyq(c.name)}))", kind="int"),
                             f"num_{plural(c.name)}", "agg_collect"))
    if ints:
        b = choice(ints)
        fb = choice(["BIT_OR", "BIT_AND"])
        opts.append((TS(f"{k(fb)}({b.name})", f"F.{fb.lower()}({pyq(b.name)})", kind="int"),
                             f"{b.name}_{fb.lower()}", "agg_bitwise"))
    tss = cur.of("ts")
    if tss:
        t = choice(tss)
        opts.append((TS(f"{k('MIN')}({t.name})", f"F.min({pyq(t.name)})", kind="ts"),
                             choice(["first_seen", f"first_{t.name}", "started_at"]), "agg_minmax_ts"))
        opts.append((TS(f"{k('MAX')}({t.name})", f"F.max({pyq(t.name)})", kind="ts"),
                             choice(["last_seen", f"last_{t.name}", "ended_at"]), "agg_minmax_ts"))
        opts.append((TS(f"({k('UNIX_TIMESTAMP')}({k('MAX')}({t.name})) - {k('UNIX_TIMESTAMP')}({k('MIN')}({t.name}))) / 60",
                               f"(F.unix_timestamp(F.max({pyq(t.name)})) - F.unix_timestamp(F.min({pyq(t.name)}))) / 60",
                               wrap=True), choice(["duration_min", "span_minutes"]), "agg_duration"))
    e, n, f = choice(opts)
    n = fresh(n, taken)
    taken.add(n)
    g.feats.add(f)
    return e, n


def make_group_key(g: Gen, cur: R, taken: set):
    """(sql_expr, py_expr, name, is_expression, Column)"""
    st = g.st
    opts = []
    cats = cur.of("cat", pooled=True) + [c for c in cur.of("fk", "int") if c.name.endswith(("_id", "_no"))]
    if cats:
        c = choice(cats)
        opts.append((c.name, pyq(c.name), c.name, False, c))
    tss = cur.of("ts", "date")
    if tss:
        t = choice(tss)
        info = ts_info(cur.base, t.name)
        dense = info is not None and info[2] < 24 * 60
        unit = choice(["hour", "day"] if dense else ["month", "week", "quarter", "year"])
        name = {"hour": "hour_window", "day": "activity_day", "month": "activity_month", "week": "week_start",
                "quarter": "quarter_start", "year": "year_start"}[unit]
        u = unit.upper() if st.upper else unit
        opts.append((f"{st.k('DATE_TRUNC')}('{u}', {t.name})", f"F.date_trunc({pyq(u)}, {pyq(t.name)})", name, True,
                             Column(name, "ts")))
    nums = [c for c in num_cols(cur) if c.hi > c.lo]
    if nums:
        v = choice(nums)
        mid = v.lo + (v.hi - v.lo) * 0.5
        cut = G.nice(mid, v.hi, v.kind)
        lab = choice([("HIGH", "LOW"), ("LARGE", "SMALL"), ("PREMIUM", "STANDARD")])
        name = f"{v.name}_tier"
        sq = (f"{st.k('CASE')} {st.k('WHEN')} {v.name} >= {fmt_num(cut)} "
              f"{st.k('THEN')} '{lab[0]}' {st.k('ELSE')} '{lab[1]}' {st.k('END')}")
        py = f"F.when(F.col({pyq(v.name)}) >= {cut}, {pyq(lab[0])}).otherwise({pyq(lab[1])})"
        opts.append((sq, py, name, True, Column(name, "cat", pool=lab)))
    if not opts:
        skip()
    sq, py, n, is_expr, col = choice(opts)
    if n in taken:
        skip()
    taken.add(n)
    return sq, py, n, is_expr, col


def stg_agg(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    k = st.k
    taken: set = set()
    nkeys = rng.choice([1, 1, 2, 2, 3])
    keys = []
    for _ in range(nkeys):
        try:
            keys.append(make_group_key(g, cur, taken))
        except G.Skip:
            pass
    if not keys:
        skip()
    taken |= {n for _, _, n, _, _ in keys}
    aggs = []
    for _ in range(rng.choice([2, 3, 3, 4, 5, 6])):
        e, n = make_agg_item(g, cur, taken)
        aggs.append((e, n))
    mode = "plain"
    if len(keys) >= 2:
        mode = wchoice([("plain", 0.6), ("grouping_sets", 0.2), ("rollup", 0.1), ("cube", 0.1)])
    elif chance(0.12):
        mode = choice(["rollup", "cube"])
    gid = mode != "plain" and chance(0.5)
    g.feats |= {"stage_agg", f"group_{mode}"}
    # SQL
    items_sql = [(ks if not is_expr else f"{ks} {k('AS')} {n}") for ks, _, n, is_expr, _ in keys]
    items_sql += [f"{e.sql(st)} {k('AS')} {n}" for e, n in aggs]
    gid_name = None
    if gid:
        gid_name = fresh(choice(["grouping_level", "gid", "grouping_id"]), taken)
        taken.add(gid_name)
        if chance(0.5):
            items_sql.append(f"{k('GROUPING_ID')}() {k('AS')} {gid_name}")
            gid_py = "F.grouping_id()"
        else:
            kk = keys[0]
            items_sql.append(f"{k('GROUPING')}({kk[0]}) {k('AS')} {gid_name}")
            gid_py = f"F.grouping({pyq(kk[2])})"
        g.feats.add("grouping_id")
    key_sqls = [ks for ks, *_ in keys]
    if mode == "plain":
        ref = wchoice([("expr", 0.6), ("alias", 0.2), ("ordinal", 0.2)])
        if ref == "expr":
            group = ", ".join(key_sqls)
        elif ref == "alias":
            group = ", ".join(n for _, _, n, _, _ in keys)
        else:
            group = ", ".join(str(i + 1) for i in range(len(keys)))
    elif mode in ("rollup", "cube"):
        group = f"{k(mode.upper())} (" + ", ".join(key_sqls) + ")"
    else:
        sets = [list(range(len(keys)))]
        sets.append(list(range(len(keys) - 1)))
        if len(keys) >= 2 and chance(0.5):
            sets.append([0])
        if chance(0.5):
            sets.append([])
        uniq = []
        for s_ in sets:
            if s_ not in uniq:
                uniq.append(s_)
        sets = uniq
        group = f"{k('GROUPING SETS')} (" + ", ".join("(" + ", ".join(key_sqls[i] for i in s_) + ")" for s_ in sets) + ")"
    having = None
    having_py = None
    num_aggs = [(e, n) for e, n in aggs if e.kind in ("int", "double")]
    if num_aggs and chance(0.3):
        e, n = choice(num_aggs)
        op = choice([">", ">=", "<"])
        val = 1 if e.kind == "int" else 0
        if chance(0.5):
            having = f"{n} {op} {val}"
        else:
            having = f"{e.sql(st)} {op} {val}"
        having_py = f".filter(F.col({pyq(n)}) {op} {val})"
        g.feats.add("having")
    # rank over the aggregate (window over GROUP BY output)
    post = None
    if num_aggs and mode == "plain" and len(keys) >= 2 and chance(0.25):
        e, n = choice(num_aggs)
        fr = choice([("RANK", "rank"), ("DENSE_RANK", "dense_rank"), ("PERCENT_RANK", "percent_rank")])
        pn = fresh(f"{n}_{fr[1]}", taken)
        taken.add(pn)
        spec_sql = f"{k('PARTITION BY')} {key_sqls[0]} {k('ORDER BY')} {e.sql(st)} {k('DESC')}"
        items_sql.append(f"{k(fr[0])}() {k('OVER')} ({spec_sql}) {k('AS')} {pn}")
        wv = window_var(ctx, f"Window.partitionBy({pyq(keys[0][2])}).orderBy(F.col({pyq(n)}).desc())")
        post = (pn, f"F.{fr[1]}().over({wv})", "double" if fr[1] == "percent_rank" else "int")
        g.feats.add("window_over_agg")
    order = []
    if last and chance(0.6):
        cand = [(n, None) for _, _, n, _, _ in keys] + [(n, e) for e, n in num_aggs]
        o1 = choice(cand)
        d = chance(0.6)
        nl = rng.choice([None, None, "last", "first"])
        order = [(o1[0], d, nl)]
        if nl:
            g.feats.add("nulls_order")
    sql = sql_select(g, items_sql, cur.name, group=group, having=having,
                     order=order_sql(g, order) if order else None)
    # PySpark
    steps = []
    pre_cols = [(n, py) for _, py, n, is_expr, _ in keys if is_expr]
    agg_args = [f"{e.py(ctx)}.alias({pyq(n)})" if not isinstance(e, T) or not e.wrap else f"({e.py(ctx)}).alias({pyq(n)})"
                for e, n in aggs]
    if gid:
        agg_args.append(f"{gid_py}.alias({pyq(gid_name)})")
    if mode == "plain":
        steps.append(call("groupBy", [(py if not is_expr else f"{py}.alias({pyq(n)})") for _, py, n, is_expr, _ in keys]))
    else:
        if pre_cols and (mode == "grouping_sets" or gid):
            steps += [f".withColumn({pyq(n)}, {py})" for n, py in pre_cols]
            key_args = [pyq(n) for _, _, n, _, _ in keys]
        else:
            key_args = [(py if not is_expr else f"{py}.alias({pyq(n)})") for _, py, n, is_expr, _ in keys]
        if mode == "grouping_sets":
            names = [pyq(n) for _, _, n, _, _ in keys]
            sets_py = "[" + ", ".join("[" + ", ".join(names[i] for i in s_) + "]" for s_ in sets) + "]"
            steps.append(call("groupingSets", [sets_py] + key_args))
        else:
            steps.append(call(mode, key_args))
    steps.append(call("agg", agg_args, width=60))
    if having_py:
        steps.append(having_py)
    if post:
        steps.append(f".withColumn({pyq(post[0])}, {post[1]})")
    if order:
        steps.append(call("orderBy", order_py_names(order)))
    cols = [col if col.name == n else Column(n, col.kind, pool=col.pool, lo=col.lo, hi=col.hi) for _, _, n, _, col in keys]
    cols += [Column(n, KIND_OF_EXPR.get(e.kind, "double"), lo=0, hi=100) for e, n in aggs]
    if gid:
        cols.append(Column(gid_name, "int", lo=0, hi=7))
    if post:
        cols.append(Column(post[0], post[2]))
    out_keys = [n for _, _, n, _, _ in keys] if mode == "plain" else None
    return Out(sql, cur.var, steps, cols, out_keys, hint=choice(["summary", "totals", "grouped", "daily", "stats"]),
               comment=choice(["Aggregate with conditional metrics", "Grouped metrics",
                               "Multi-level aggregation"]), order=[o[0] for o in order])


def stg_setop(g: Gen, cur: R, last: bool) -> Out:
    """Key set difference / intersection (EXCEPT [ALL|DISTINCT], INTERSECT [ALL]) and, unless last, a join back."""
    st, ctx = g.st, g.ctx
    k = st.k
    if cur.base is None:
        skip()
    kc = [c for c in cur.cols if c.kind in ("fk", "cat", "id") and c.name in [x.name for x in cur.base.columns]]
    if len(kc) < 1:
        skip()
    ks = rng.sample(kc, min(len(kc), rng.choice([1, 2, 2])))
    names = [c.name for c in ks]
    from sqlspec import Rel
    base = cur.base
    brel = Rel(base.name, f"df_{base.name}", [c for c in base.columns if c.kind in PLAIN_KINDS and
                                              (c.kind not in ("cat", "str") or c.pool)], table=base)
    p, f = G.atom(brel)
    op = wchoice([("EXCEPT", 0.25), ("EXCEPT DISTINCT", 0.15), ("EXCEPT ALL", 0.2), ("INTERSECT", 0.2),
                  ("INTERSECT ALL", 0.2)])
    method = {"EXCEPT": "subtract", "EXCEPT DISTINCT": "subtract", "EXCEPT ALL": "exceptAll",
              "INTERSECT": "intersect", "INTERSECT ALL": "intersectAll"}[op]
    g.feats |= {"stage_setop", "set_" + op.lower().replace(" ", "_")}
    left = sql_select(g, names, cur.name)
    right = sql_select(g, names, base.name, where=[(f"({p.sql(st)})" if isinstance(p, Or) else p.sql(st))])
    nl = "\n" if st.multiline else " "
    set_sql = f"{left}{nl}{k(op)}{nl}{right}"
    right_py = chain(f"df_{base.name}", [f".filter({p.py(ctx)})", call("select", [pyq(n) for n in names])])
    set_py_steps = [call("select", [pyq(n) for n in names]), f".{method}({as_arg(right_py)})"]
    if last:
        return Out(set_sql, cur.var, set_py_steps, [c for c in cur.cols if c.name in names], None,
                   hint="keys", comment=None)
    kname = fresh(choice(["kept_keys", "matched_keys", "remaining_keys"]), g.names)
    g.names.add(kname)
    sa, ka = "s", "k"
    on = f" {k('AND')} ".join(f"{sa}.{n} = {ka}.{n}" for n in names)
    join_sql = f"{k(st.inner_kw)} {kname} {ka} {k('ON')} {on}"
    sql = sql_select(g, [f"{sa}.*"], f"{cur.name} {sa}", joins=[join_sql])
    cond = " & ".join(f"(F.col({pyq(sa + '.' + n)}) == F.col({pyq(ka + '.' + n)}))" for n in names)
    steps = [f".alias({pyq(sa)})", f".join({kname}.alias({pyq(ka)}), {cond}, \"inner\")", f".select({pyq(sa + '.*')})"]
    extra = [(kname, kname, set_sql, chain(cur.var, set_py_steps), f"Set {'difference' if 'EXCEPT' in op else 'intersection'} on keys")]
    return Out(sql, cur.var, steps, list(cur.cols), None, hint=choice(["reconciled", "kept_rows", "verified"]),
               comment="Keep rows whose keys survive the set operation", extra=extra)


def stg_pivot(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    k = st.k
    pcs = [c for c in cur.of("cat", pooled=True) if len(c.pool) >= 2]
    nums = num_cols(cur)
    if not pcs or not nums:
        skip()
    pc = choice(pcs)
    gks = [c for c in cur.of("cat", "fk", pooled=False) if c.name != pc.name and (c.kind == "fk" or c.pool)]
    if not gks:
        skip()
    gk = rng.sample(gks, min(len(gks), rng.choice([1, 1, 2])))
    v = choice(nums)
    vals = rng.sample(list(pc.pool), min(len(pc.pool), rng.choice([2, 2, 3])))
    ident_ok = all(x.replace("_", "").isalnum() and not x[0].isdigit() for x in vals)
    alias_vals = (not ident_ok) or chance(0.4)
    valias = [G.ident(x) if alias_vals else x for x in vals]
    valias = [("v_" + a if a[0].isdigit() else a) for a in valias]
    two = chance(0.45)
    a1 = choice(["total", "amount", "value_sum"]) if two or chance(0.4) else None
    fa = choice(["SUM", "AVG", "MAX"])
    aggs_sql = [f"{k(fa)}({v.name})" + (f" {k('AS')} {a1}" if a1 else "")]
    aggs_py = [f"F.{fa.lower()}({pyq(v.name)})" + (f".alias({pyq(a1)})" if a1 else "")]
    a2 = None
    if two:
        a2 = choice(["cnt", "n", "item_count"])
        aggs_sql.append(f"{k('COUNT')}(1) {k('AS')} {a2}")
        aggs_py.append(f"F.count(F.lit(1)).alias({pyq(a2)})")
    in_sql = ", ".join(f"'{x}'" + (f" {k('AS')} {a}" if alias_vals else "") for x, a in zip(vals, valias))
    src_items = [c.name for c in gk] + [pc.name, v.name]
    inner = sql_select(g, src_items, cur.name)
    if st.multiline:
        inner = "\n".join("    " + line for line in inner.split("\n"))
        frm = (f"(\n{inner}\n) src\n{k('PIVOT')} (\n    " + ",\n    ".join(aggs_sql)
               + f"\n    {k('FOR')} {pc.name} {k('IN')} ({in_sql})\n)")
    else:
        frm = f"({inner}) src {k('PIVOT')} (" + ", ".join(aggs_sql) + f" {k('FOR')} {pc.name} {k('IN')} ({in_sql}))"
    sql = sql_select(g, ["*"], frm)
    # PySpark: groupBy + pivot, then rename to the SQL column names
    py_names, sql_names = [], []
    for x, a in zip(vals, valias):
        if two:
            for an in (a1, a2):
                py_names.append(f"{x}_{an}")
                sql_names.append(f"{a}_{an}")
        else:
            py_names.append(x)
            sql_names.append(a)
    steps = [call("groupBy", [pyq(c.name) for c in gk]),
             f".pivot({pyq(pc.name)}, [{', '.join(pyq(x) for x in vals)}])",
             call("agg", aggs_py)]
    if py_names != sql_names:
        steps.append(call("select", [pyq(c.name) for c in gk] + [
            f"F.col({pyq(p)}).alias({pyq(s)})" if p != s else pyq(p) for p, s in zip(py_names, sql_names)]))
    cols = [Column(c.name, c.kind, pool=c.pool, lo=c.lo, hi=c.hi, ref=c.ref) for c in gk]
    cols += [Column(n, "double" if (n.endswith(("_" + (a1 or ""),)) or not two) and fa != "COUNT" else "int", lo=0, hi=100)
             for n in sql_names]
    g.feats |= {"stage_pivot", "pivot", "pivot_multi_agg" if two else "pivot_single", "pivot_alias" if alias_vals else "pivot_values"}
    return Out(sql, cur.var, steps, cols, [c.name for c in gk], hint=choice(["pivoted", "by_type", "wide"]),
               comment="Native SQL PIVOT")


def stg_unpivot(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    k = st.k
    dbls = cur.of("double")
    if len(dbls) < 2:
        skip()
    vs = rng.sample(dbls, min(len(dbls), rng.choice([2, 3, 3, 4])))
    ids = [n for n in (cur.keys or [])] + [c.name for c in rng.sample(part_candidates(cur), min(len(part_candidates(cur)), 1))
                                            if c.name not in (cur.keys or [])]
    if not ids:
        skip()
    aliased = chance(0.5)
    al = [(c.name.upper() if not aliased else choice([c.name.upper(), G.ident(c.name).upper() + "_VAL"])) for c in vs]
    use_alias = [aliased and chance(0.7) for _ in vs]
    labels = [a if u else c.name for c, a, u in zip(vs, al, use_alias)]
    if len(set(labels)) < len(labels):
        skip()
    include_nulls = chance(0.25)
    mname = choice(["metric_name", "metric", "measure", "kpi"])
    vname = choice(["metric_value", "value", "reading", "val"])
    if mname in ids or vname in ids:
        skip()
    inner = sql_select(g, ids + [c.name for c in vs], cur.name)
    in_items = ", ".join(c.name + (f" {k('AS')} {a}" if u else "") for c, a, u in zip(vs, al, use_alias))
    un = k("UNPIVOT") + (" " + k("INCLUDE NULLS") if include_nulls else "")
    if st.multiline:
        inner = "\n".join("    " + line for line in inner.split("\n"))
        frm = f"(\n{inner}\n) src\n{un} (\n    {vname} {k('FOR')} {mname} {k('IN')} ({in_items})\n)"
    else:
        frm = f"({inner}) src {un} ({vname} {k('FOR')} {mname} {k('IN')} ({in_items}))"
    sql = sql_select(g, ["*"], frm)
    sel = [pyq(i) for i in ids] + [(f"F.col({pyq(c.name)}).alias({pyq(lb)})" if lb != c.name else pyq(c.name))
                                    for c, lb in zip(vs, labels)]
    steps = [call("select", sel),
             call("unpivot", [f"[{', '.join(pyq(i) for i in ids)}]", f"[{', '.join(pyq(lb) for lb in labels)}]",
                              pyq(mname), pyq(vname)])]
    if not include_nulls:
        steps.append(f".filter(F.col({pyq(vname)}).isNotNull())")
    cols = [cur.col(i) for i in ids] + [Column(mname, "cat", pool=tuple(labels), nullable=False), Column(vname, "double")]
    keys = (ids + [mname]) if cur.keys and all(x in ids for x in cur.keys) else None
    g.feats |= {"stage_unpivot", "unpivot", "unpivot_include_nulls" if include_nulls else "unpivot_exclude_nulls"}
    return Out(sql, cur.var, steps, cols, keys, hint=choice(["long_format", "unpivoted", "metrics_long"]),
               comment="Native SQL UNPIVOT (wide to long)")


def stg_lateral(g: Gen, cur: R, last: bool) -> Out:
    """[LEFT] JOIN LATERAL (... ORDER BY ... LIMIT 1) -> join + ROW_NUMBER per source row."""
    st, ctx = g.st, g.ctx
    k = st.k
    if not cur.keys or not cur.has("id") or cur.base is None:
        skip()
    kids = [(t, c) for t, c in children_of(cur.base) if any(x.kind in ("date", "ts", "int", "double") and x.name != "id"
                                                          for x in t.columns)]
    if not kids:
        skip()
    child, fkc = choice(kids)
    sa, ca, la = "s", "c", choice(["latest", "top1", "x"])
    oc = choice([x for x in child.columns if x.kind in ("date", "ts", "int", "double") and x.name not in ("id", fkc.name)])
    outs = [x for x in child.columns if x.kind in ("cat", "int", "double", "date", "ts") and x.name not in ("id", fkc.name)]
    picked = rng.sample(outs, min(len(outs), rng.choice([1, 2])))
    taken = set(cur.names())
    names = []
    for x in picked:
        n = fresh(f"{G.singular(child.name)}_{x.name}" if x.name in taken or chance(0.5) else x.name, taken)
        taken.add(n)
        names.append((x, n))
    conds_sql = [f"{ca}.{fkc.name} = {sa}.id"]
    conds_py = [f"(F.col({pyq(ca + '.' + fkc.name)}) == F.col({pyq(sa + '.id')}))"]
    sd = cur.of("date", "ts")
    if sd and oc.kind in ("date", "ts") and chance(0.5):
        b = choice(sd)
        conds_sql.append(f"{ca}.{oc.name} <= {sa}.{b.name}")
        conds_py.append(f"(F.col({pyq(ca + '.' + oc.name)}) <= F.col({pyq(sa + '.' + b.name)}))")
    left = chance(0.6)
    k_ = k
    sub_items = ", ".join(f"{ca}.{x.name}" + ("" if n == x.name else f" {k_('AS')} {n}") for x, n in names)
    sub = (f"{k('SELECT')} {sub_items} {k('FROM')} {child.name} {ca} {k('WHERE')} " + f" {k('AND')} ".join(conds_sql)
           + f" {k('ORDER BY')} {ca}.{oc.name} {k('DESC')}, {ca}.id {k('DESC')} {k('LIMIT')} 1")
    if st.multiline:
        sub = (f"{k('SELECT')} {sub_items}\n        {k('FROM')} {child.name} {ca}\n        {k('WHERE')} "
               + f"\n          {k('AND')} ".join(conds_sql)
               + f"\n        {k('ORDER BY')} {ca}.{oc.name} {k('DESC')}, {ca}.id {k('DESC')}\n        {k('LIMIT')} 1")
        join_sql = f"{k('LEFT JOIN LATERAL') if left else k('JOIN LATERAL')} (\n        {sub}\n    ) {la} {k('ON')} {k('TRUE')}"
    else:
        join_sql = f"{k('LEFT JOIN LATERAL') if left else k('JOIN LATERAL')} ({sub}) {la} {k('ON')} {k('TRUE')}"
    items_sql = [f"{sa}.*"] + [f"{la}.{n}" for _, n in names]
    sql = sql_select(g, items_sql, f"{cur.name} {sa}", joins=[join_sql])
    wspec = (f"Window.partitionBy({', '.join('F.col(' + pyq(sa + '.' + kk) + ')' for kk in cur.keys)})"
             f".orderBy(F.col({pyq(ca + '.' + oc.name)}).desc(), F.col({pyq(ca + '.id')}).desc())")
    wv = window_var(ctx, wspec)
    rn = fresh("rn", taken)
    steps = [f".alias({pyq(sa)})",
             f".join(df_{child.name}.alias({pyq(ca)}), {' & '.join(conds_py)}, {pyq('left' if left else 'inner')})",
             f".withColumn({pyq(rn)}, F.row_number().over({wv}))",
             f".filter(F.col({pyq(rn)}) == 1)",
             call("select", [pyq(sa + ".*")] + [f"F.col({pyq(ca + '.' + x.name)})" + (f".alias({pyq(n)})" if n != x.name else "")
                                               for x, n in names])]
    cols = list(cur.cols) + [Column(n, x.kind, pool=x.pool, lo=x.lo, hi=x.hi) for x, n in names]
    g.feats |= {"stage_lateral", "lateral_join", "lateral_left" if left else "lateral_inner", "window_row_number"}
    return Out(sql, cur.var, steps, cols, cur.keys, hint=choice(["with_latest", "latest_match", "with_last"]),
               comment=f"Latest matching {G.singular(child.name)} per row (LATERAL ... LIMIT 1)")


SCALAR_SOURCES = {}   # column name -> [(table, numeric col)]


def scalar_sources(colname):
    if not SCALAR_SOURCES:
        for t in train_tables():
            for c in t.columns:
                if c.kind == "fk":
                    nums = [x for x in t.columns if x.kind in ("int", "double") and x.name != c.name]
                    if nums:
                        SCALAR_SOURCES.setdefault(c.name, []).append((t, nums))
    return SCALAR_SOURCES.get(colname, [])


def stg_final(g: Gen, cur: R, last: bool) -> Out:
    """Final projection: CASE + RLIKE, correlated scalar subquery in SELECT, ORDER BY with NULLS, LIMIT."""
    st, ctx = g.st, g.ctx
    k = st.k
    taken = set(cur.names())
    keep = [c for c in cur.cols if c.kind in PLAIN_KINDS]
    if not keep:
        skip()
    keep = [c for c in keep if chance(0.7) or c.name in (cur.keys or [])][:9] or keep[:4]
    items = [(C(c.name, c.kind), c.name, c) for c in keep]
    taken |= {c.name for c in keep}
    extras_sql, extras_py = [], []
    pre_join = None
    # CASE with RLIKE
    strs = [c for c in keep if c.kind in ("cat", "str") and c.pool]
    if strs and chance(0.45):
        c = choice(strs)
        vals = [v for v in c.pool if v.replace(" ", "").replace("_", "").replace("-", "").isalnum()]
        if len(vals) >= 3:
            a = rng.sample(vals, 2)
            b = [v for v in vals if v not in a][:2]
            pat1 = "^(" + "|".join(a) + ")$"
            pat2 = "^(" + "|".join(b) + ")$" if b and chance(0.6) else None
            lab = rng.sample(["GROUP_A", "GROUP_B", "PRIORITY", "CORE", "EXTENDED", "OTHER_ZONE"], 3)
            n = fresh(f"{c.name}_zone", taken)
            taken.add(n)
            branches = [(rlike(C(c.name, "cat"), pat1), Lit(lab[0]))] + ([(rlike(C(c.name, "cat"), pat2), Lit(lab[1]))] if pat2 else [])
            e = Case(branches, Lit(lab[2]))
            items.append((e, n, Column(n, "cat", pool=tuple(lab))))
            g.feats.add("rlike")
    # correlated scalar subquery
    fks_ = [c for c in keep if c.kind == "fk" and scalar_sources(c.name)]
    if fks_ and chance(0.45):
        c = choice(fks_)
        t, nums = choice(scalar_sources(c.name))
        v = choice(nums)
        fn = choice(["MAX", "SUM", "AVG", "MIN"])
        n = fresh(f"{fn.lower()}_{G.singular(t.name)}_{v.name}", taken)
        taken.add(n)
        sa = "s"
        ta = choice(["x", "t2", t.name[0] + "2"])
        sub = f"({k('SELECT')} {k(fn)}({ta}.{v.name}) {k('FROM')} {t.name} {ta} {k('WHERE')} {ta}.{c.name} = {sa}.{c.name})"
        lookup = fresh(f"{v.name}_by_{c.name}", g.names | {"result"})
        g.names.add(lookup)
        pre_join = (lookup, c.name, n, f"df_{t.name}.groupBy({pyq(c.name)}).agg(F.{fn.lower()}({pyq(v.name)}).alias({pyq(n)}))")
        items.append((T(lambda s_, sub=sub: sub, lambda x, n=n: f"F.col({pyq(n)})"), n, Column(n, "double")))
        g.feats.add("scalar_subquery_select")
    order = []
    if chance(0.7):
        cands = [n for _, n, col in items if col.kind in ("int", "double", "date", "ts", "cat", "fk", "id")]
        if cands:
            o1 = choice(cands)
            nl = rng.choice([None, None, "last", "first"])
            order.append((o1, chance(0.6), nl))
            if nl:
                g.feats.add("nulls_order")
            if chance(0.3):
                o2 = choice([c for c in cands if c != o1] or [o1])
                if o2 != o1:
                    order.append((o2, chance(0.4), None))
    limit = None
    if order and cur.keys and chance(0.25) and all(any(o[0] == kk for o in order) for kk in cur.keys):
        limit = rng.choice([5, 10, 20])
    sa = "s" if pre_join else None
    frm = f"{cur.name} {sa}" if sa else cur.name
    items_sql = []
    for e, n, col in items:
        if isinstance(e, Col) and e.name == n:
            items_sql.append(f"{sa}.{n}" if sa else n)
        elif sa and not isinstance(e, T):
            items_sql.append(f"{_qualify_sql(e, sa, st)} {k('AS')} {n}")
        else:
            items_sql.append(f"{e.sql(st)} {k('AS')} {n}")
    sql = sql_select(g, items_sql, frm, order=order_sql(g, order) if order else None, limit=limit)
    steps = []
    head = cur.var
    if pre_join:
        lookup, key, n, expr = pre_join
        g.ctx.pre.append(f"{lookup} = {expr}")
        steps.append(f".join({lookup}, on={pyq(key)}, how=\"left\")")
    steps.append(call("select", [sel_py(e, n, ctx) if not isinstance(e, T) else f"F.col({pyq(n)})" for e, n, _ in items]))
    if order:
        steps.append(call("orderBy", order_py_names(order)))
    if limit:
        steps.append(f".limit({limit})")
    cols = [col for _, _, col in items]
    g.feats.add("stage_final")
    return Out(sql, head, steps, cols, None, hint="report", order=[o[0] for o in order])


# ---------------------------------------------------------------------------
# Stages (nested data)
# ---------------------------------------------------------------------------
NESTED_FIELDS = {
    "items": [("product_id", "int", 1, 12, ()), ("qty", "int", 1, 6, ()), ("price", "double", 5, 300, ()),
              ("promo", "cat", 0, 0, PROMO_CODES)],
    "ship_to": [("city", "cat", 0, 0, ("Bangkok", "Chiang Mai", "New York", "Singapore", "Tokyo", "London", "Berlin")),
                ("zip", "str", 0, 0, ()), ("country", "cat", 0, 0, ("TH", "US", "SG", "JP", "UK", "DE"))],
}
MAP_KEYS = {"attributes": [("device", False), ("utm_source", False), ("gift", False), ("weight_kg", True)],
            "context": [("region", False), ("version", False), ("trace", False)]}
PAYLOAD_SCHEMA = "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>"


def stg_explode(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    k = st.k
    arrs = [c for c in cur.cols if c.kind in ("array_struct", "array", "map", "array_double", "array_int")]
    if not arrs:
        skip()
    a = choice(arrs)
    keepc = [c for c in cur.cols if c.kind in PLAIN_KINDS and c.name != a.name]
    keep = [c for c in keepc if c.name in (cur.keys or []) or chance(0.6)] or keepc[:2]
    taken = set(cur.names())
    outer = chance(0.5)
    ta = choice(["t", "li", "x", "e"])
    if a.kind == "array_struct":
        form = wchoice([("lateral", 0.45), ("posexplode", 0.25), ("inline", 0.15), ("select_explode", 0.15)])
        item = choice(["item", "line", "li_item", "elem"])
        flds = NESTED_FIELDS.get(a.name) or skip()
        picked = [f for f in flds if chance(0.75)] or flds[:2]
        outs = []
        for fn_, kd, lo, hi, pool in picked:
            n = fresh(fn_ if fn_ not in taken else f"{G.singular(a.name)}_{fn_}", taken)
            taken.add(n)
            outs.append((fn_, n, Column(n, kd, pool=pool, lo=lo, hi=hi)))
        if form == "inline":
            outer = False
            sql = sql_select(g, [c.name for c in keep] + [f"{k('INLINE')}({a.name})"], cur.name)
            steps = [call("select", [pyq(c.name) for c in keep] + [f"F.inline({pyq(a.name)})"])]
            cols = keep + [Column(fn_, kd, pool=pool, lo=lo, hi=hi) for fn_, kd, lo, hi, pool in flds]
            if any(fn_ in {c.name for c in keep} for fn_, *_ in flds):
                skip()
            g.feats |= {"inline"}
            keys = None
        elif form == "select_explode":
            fn = "EXPLODE_OUTER" if outer else "EXPLODE"
            inner = sql_select(g, [c.name for c in keep] + [f"{k(fn)}({a.name}) {k('AS')} {item}"], cur.name)
            if st.multiline:
                inner = "\n".join("    " + line for line in inner.split("\n"))
                frm = f"(\n{inner}\n) {ta}"
            else:
                frm = f"({inner}) {ta}"
            sql = sql_select(g, [c.name for c in keep] + [f"{item}.{f_}" + ("" if n == f_ else f" {k('AS')} {n}") for f_, n, _ in outs], frm)
            steps = [f".withColumn({pyq(item)}, F.{'explode_outer' if outer else 'explode'}({pyq(a.name)}))",
                     call("select", [pyq(c.name) for c in keep] + [f"F.col({pyq(item + '.' + f_)})" + (f".alias({pyq(n)})" if n != f_ else "") for f_, n, _ in outs])]
            cols = keep + [c for _, _, c in outs]
            keys = None
            g.feats.add("explode_select")
        else:
            pos = form == "posexplode"
            fn = ("POSEXPLODE" if pos else "EXPLODE")
            lv = f"{k('LATERAL VIEW')} {k('OUTER') + ' ' if outer else ''}{k(fn)}({a.name}) {ta} {k('AS')} " + (f"pos, {item}" if pos else item)
            items_sql = [c.name for c in keep] + (["pos"] if pos else []) + [f"{item}.{f_}" + ("" if n == f_ else f" {k('AS')} {n}") for f_, n, _ in outs]
            sql = sql_select(g, items_sql, cur.name, joins=[lv])
            if pos:
                pf = "posexplode_outer" if outer else "posexplode"
                steps = [call("select", ['"*"', f"F.{pf}({pyq(a.name)}).alias(\"pos\", {pyq(item)})"])]
            else:
                steps = [f".withColumn({pyq(item)}, F.{'explode_outer' if outer else 'explode'}({pyq(a.name)}))"]
            steps.append(call("select", [pyq(c.name) for c in keep] + (['"pos"'] if pos else []) +
                              [f"F.col({pyq(item + '.' + f_)})" + (f".alias({pyq(n)})" if n != f_ else "") for f_, n, _ in outs]))
            cols = keep + ([Column("pos", "int", lo=0, hi=4)] if pos else []) + [c for _, _, c in outs]
            keys = (cur.keys + ["pos"]) if pos and cur.keys and all(x in [c.name for c in keep] for x in cur.keys) and not outer else None
            g.feats |= {"lateral_view", "posexplode" if pos else "explode"}
    elif a.kind == "map":
        kn, vn = fresh(choice(["attr_key", "key", "k"]), taken), None
        taken.add(kn)
        vn = fresh(choice(["attr_value", "value", "v"]), taken)
        taken.add(vn)
        fn = "EXPLODE_OUTER" if outer else "EXPLODE"
        lv = f"{k('LATERAL VIEW')} {k('OUTER') + ' ' if outer else ''}{k('EXPLODE')}({a.name}) {ta} {k('AS')} {kn}, {vn}"
        sql = sql_select(g, [c.name for c in keep] + [kn, vn], cur.name, joins=[lv])
        steps = [call("select", [pyq(c.name) for c in keep] + [f"F.{fn.lower()}({pyq(a.name)}).alias({pyq(kn)}, {pyq(vn)})"])]
        pool = tuple(x for x, _ in MAP_KEYS.get(a.name, []))
        cols = keep + [Column(kn, "cat", pool=pool), Column(vn, "str")]
        keys = (cur.keys + [kn]) if cur.keys and not outer and all(x in [c.name for c in keep] for x in cur.keys) else None
        g.feats |= {"lateral_view", "explode_map"}
    else:
        n = fresh(choice(["code", "tag", "value", "element"]) if a.kind == "array" else choice(["tier_value", "element", "val"]), taken)
        taken.add(n)
        fn = "EXPLODE_OUTER" if outer else "EXPLODE"
        if chance(0.5):
            sql = sql_select(g, [c.name for c in keep] + [f"{k(fn)}({a.name}) {k('AS')} {n}"], cur.name)
        else:
            lv = f"{k('LATERAL VIEW')} {k('OUTER') + ' ' if outer else ''}{k('EXPLODE')}({a.name}) {ta} {k('AS')} {n}"
            sql = sql_select(g, [c.name for c in keep] + [n], cur.name, joins=[lv])
            g.feats.add("lateral_view")
        steps = [call("select", [pyq(c.name) for c in keep] + [f"F.{fn.lower()}({pyq(a.name)}).alias({pyq(n)})"])]
        kind = {"array": "cat", "array_double": "double", "array_int": "int"}[a.kind]
        cols = keep + [Column(n, kind, pool=a.pool if a.kind == "array" else (), lo=0, hi=100)]
        keys = None
        g.feats.add("explode_array")
    if outer:
        g.feats.add("explode_outer")
    g.feats.add("stage_explode")
    return Out(sql, cur.var, steps, cols, keys, hint=choice(["exploded", "lines", "flattened"]),
               comment="Unnest the array into one row per element")


def nested_exprs(g: Gen, cur: R, taken: set):
    """Candidate nested / semi-structured expressions for the columns of `cur`: [(E, name, Column, feat)]."""
    out = []
    for c in cur.cols:
        q = C(c.name, c.kind)
        if c.kind == "struct" and c.name in NESTED_FIELDS:
            for f_, kd, lo, hi, pool in NESTED_FIELDS[c.name]:
                out.append((field_of(q, f_), f"{c.name.split('_')[0]}_{f_}", Column(f"x", kd, pool=pool), "struct_field"))
            f_ = choice(NESTED_FIELDS[c.name])
            out.append((Fn("COALESCE", "coalesce", [field_of(q, f_[0]), Lit("UNKNOWN")], kind="str"),
                        f"{f_[0]}_or_unknown", Column("x", "cat"), "struct_field"))
        elif c.kind == "map":
            for key, numeric in MAP_KEYS.get(c.name, []):
                if numeric:
                    out.append((try_cast(map_get(q, key), "DOUBLE"), key, Column("x", "double", lo=0, hi=5), "map_try_cast"))
                else:
                    out.append((map_get(q, key), key, Column("x", "cat"), "map_access"))
                    out.append((Fn("COALESCE", "coalesce", [map_get(q, key), Lit(choice(["direct", "unknown", "none"]))], kind="str"),
                                f"{key}_or_default", Column("x", "cat"), "map_access"))
            out.append((T(lambda s, c=c: f"{s.k('SIZE')}({c.name})", lambda x, c=c: f"F.size({pyq(c.name)})", kind="int"),
                        f"num_{c.name}", Column("x", "int"), "map_size"))
            key = choice(MAP_KEYS.get(c.name, [("x", False)]))[0]
            out.append((T(lambda s, c=c, key=key: f"{s.k('ARRAY_CONTAINS')}({s.k('MAP_KEYS')}({c.name}), '{key}')",
                          lambda x, c=c, key=key: f"F.array_contains(F.map_keys({pyq(c.name)}), {pyq(key)})", kind="bool"),
                        f"has_{key}", Column("x", "bool"), "map_keys"))
        elif c.kind == "json":
            for path, n, kd in (("$.user.tier", "user_tier", "cat"), ("$.user.id", "user_id", "str"),
                                ("$.duration", "duration_text", "str")):
                out.append((T(lambda s, c=c, path=path: f"{s.k('GET_JSON_OBJECT')}({c.name}, '{path}')",
                              lambda x, c=c, path=path: f"F.get_json_object({pyq(c.name)}, {pyq(path)})", kind="str"),
                            n, Column("x", kd), "get_json_object"))
            for fld, n, kd in (("duration", "duration_sec", "double"), ("retries", "retry_count", "int")):
                out.append((T(lambda s, c=c, fld=fld: f"{s.k('FROM_JSON')}({c.name}, '{PAYLOAD_SCHEMA}').{fld}",
                              lambda x, c=c, fld=fld: f"F.from_json({pyq(c.name)}, {pyq(PAYLOAD_SCHEMA)})[{pyq(fld)}]", kind=kd),
                            n, Column("x", kd, lo=0, hi=5), "from_json"))
            out.append((T(lambda s, c=c: f"{s.k('FROM_JSON')}({c.name}, '{PAYLOAD_SCHEMA}').user.tier",
                          lambda x, c=c: f"F.from_json({pyq(c.name)}, {pyq(PAYLOAD_SCHEMA)})[\"user\"][\"tier\"]", kind="str"),
                        "tier", Column("x", "cat", pool=("free", "gold", "pro")), "from_json"))
        elif c.kind == "array_struct" and c.name == "items":
            v = choice(["x", "i", "li", "item"])
            out += [
                (T(lambda s, v=v: f"{s.k('AGGREGATE')}(items, {rng_zero(s)}, (acc, {v}) -> acc + {v}.qty * {v}.price, acc -> {s.k('ROUND')}(acc, 2))",
                   lambda x, v=v: f"F.aggregate(\"items\", F.lit(0.0), lambda acc, {v}: acc + {v}[\"qty\"] * {v}[\"price\"], lambda acc: F.round(acc, 2))"),
                 choice(["cart_value", "items_total", "basket_amount"]), Column("x", "double", lo=0, hi=2000), "lambda_aggregate"),
                (T(lambda s, v=v: f"{s.k('TRANSFORM')}(items, {v} -> {v}.product_id)",
                   lambda x, v=v: f"F.transform(\"items\", lambda {v}: {v}[\"product_id\"])", kind="array"),
                 "product_ids", Column("x", "array_int", dtype="ARRAY<INT>"), "lambda_transform"),
                (T(lambda s, v=v: f"{s.k('SIZE')}({s.k('FILTER')}(items, {v} -> {v}.qty >= 3))",
                   lambda x, v=v: f"F.size(F.filter(\"items\", lambda {v}: {v}[\"qty\"] >= 3))", kind="int"),
                 "bulk_lines", Column("x", "int", lo=0, hi=4), "lambda_filter"),
                (T(lambda s, v=v: f"{s.k('EXISTS')}(items, {v} -> {v}.price > 100)",
                   lambda x, v=v: f"F.exists(\"items\", lambda {v}: {v}[\"price\"] > 100)", kind="bool"),
                 "has_premium_item", Column("x", "bool"), "lambda_exists"),
                (T(lambda s, v=v: f"{s.k('FORALL')}(items, {v} -> {v}.qty <= 5)",
                   lambda x, v=v: f"F.forall(\"items\", lambda {v}: {v}[\"qty\"] <= 5)", kind="bool"),
                 "all_small_qty", Column("x", "bool"), "lambda_forall"),
                (T(lambda s, v=v: f"{s.k('ARRAY_MAX')}({s.k('TRANSFORM')}(items, {v} -> {v}.price))",
                   lambda x, v=v: f"F.array_max(F.transform(\"items\", lambda {v}: {v}[\"price\"]))"),
                 "max_item_price", Column("x", "double", lo=5, hi=300), "lambda_transform"),
                (T(lambda s, v=v: (f"{s.k('TRANSFORM')}(\n        {s.k('FILTER')}(items, {v} -> {v}.promo {s.k('IS NOT NULL')}),\n"
                                   f"        {v} -> {s.k('STRUCT')}({v}.product_id {s.k('AS')} pid, {s.k('ROUND')}({v}.price * 0.9, 2) {s.k('AS')} discounted_price)\n    )")
                   if s.multiline else
                   f"{s.k('TRANSFORM')}({s.k('FILTER')}(items, {v} -> {v}.promo {s.k('IS NOT NULL')}), {v} -> {s.k('STRUCT')}({v}.product_id {s.k('AS')} pid, {s.k('ROUND')}({v}.price * 0.9, 2) {s.k('AS')} discounted_price))",
                   lambda x, v=v: (f"F.transform(\n        F.filter(\"items\", lambda {v}: {v}[\"promo\"].isNotNull()),\n"
                                   f"        lambda {v}: F.struct({v}[\"product_id\"].alias(\"pid\"), F.round({v}[\"price\"] * 0.9, 2).alias(\"discounted_price\")),\n    )"),
                   kind="array"),
                 "promo_lines", Column("x", "array_struct", dtype="ARRAY<STRUCT<pid: INT, discounted_price: DOUBLE>>"),
                 "lambda_struct"),
                (T(lambda s: f"{s.k('SIZE')}(items)", lambda x: 'F.size("items")', kind="int"),
                 "num_items", Column("x", "int", lo=0, hi=4), "array_size"),
            ]
        elif c.kind == "array" and c.name == "coupon_codes":
            code = choice(PROMO_CODES)
            out += [
                (T(lambda s, code=code: f"{s.k('ARRAY_CONTAINS')}(coupon_codes, '{code}')",
                   lambda x, code=code: f"F.array_contains(\"coupon_codes\", {pyq(code)})", kind="bool"),
                 f"used_{code.lower()}", Column("x", "bool"), "array_contains"),
                (T(lambda s: f"{s.k('ARRAY_JOIN')}({s.k('ARRAY_SORT')}(coupon_codes), ',')",
                   lambda x: 'F.array_join(F.array_sort("coupon_codes"), ",")', kind="str"),
                 "coupons_text", Column("x", "str"), "array_join"),
                (T(lambda s: f"{s.k('EXISTS')}(coupon_codes, c -> c {s.k('LIKE')} 'W%')",
                   lambda x: 'F.exists("coupon_codes", lambda c: c.like("W%"))', kind="bool"),
                 "has_welcome_code", Column("x", "bool"), "lambda_exists"),
            ]
        elif c.kind == "array_double" and c.name == "tier_prices":
            if cur.has("discounts"):
                out.append((T(lambda s: f"{s.k('ZIP_WITH')}(tier_prices, discounts, (p, d) -> {s.k('ROUND')}(p * (1 - d), 2))",
                              lambda x: 'F.zip_with("tier_prices", "discounts", lambda p, d: F.round(p * (1 - d), 2))', kind="array"),
                            "net_prices", Column("x", "array_double", dtype="ARRAY<DOUBLE>"), "lambda_zip_with"))
            if cur.has("tier_limits"):
                out += [
                    (T(lambda s: f"{s.k('TRANSFORM')}(tier_prices, (p, i) -> {s.k('ROUND')}(p * tier_limits[i], 2))",
                       lambda x: 'F.transform("tier_prices", lambda p, i: F.round(p * F.col("tier_limits")[i], 2))', kind="array"),
                     "tier_revenue_caps", Column("x", "array_double", dtype="ARRAY<DOUBLE>"), "lambda_transform_index"),
                    (T(lambda s: f"{s.k('SLICE')}(tier_limits, 1, 2)", lambda x: 'F.slice("tier_limits", 1, 2)', kind="array"),
                     "first_two_limits", Column("x", "array_int", dtype="ARRAY<INT>"), "array_slice"),
                ]
            out += [
                (T(lambda s: f"{s.k('AGGREGATE')}(tier_prices, {rng_zero(s)}, (acc, p) -> acc + p, acc -> {s.k('ROUND')}(acc / {s.k('SIZE')}(tier_prices), 2))",
                   lambda x: 'F.aggregate("tier_prices", F.lit(0.0), lambda acc, p: acc + p, lambda acc: F.round(acc / F.size("tier_prices"), 2))'),
                 "avg_tier_price", Column("x", "double", lo=0, hi=40), "lambda_aggregate"),
                (T(lambda s: f"{s.k('ELEMENT_AT')}(tier_prices, 1)", lambda x: 'F.element_at("tier_prices", 1)'),
                 "entry_price", Column("x", "double", lo=0, hi=40), "element_at"),
                (T(lambda s: f"{s.k('ARRAY_MIN')}(tier_prices)", lambda x: 'F.array_min("tier_prices")'),
                 "lowest_price", Column("x", "double", lo=0, hi=40), "array_min"),
            ]
        elif c.kind == "int" and c.name in ("status_bits", "status_code"):
            bit = choice([1, 2, 4, 8])
            out.append((cast_int(bit_flag(q, bit)), choice([f"flag_bit{bit}", "has_fault", "is_flagged"]),
                        Column("x", "int", lo=0, hi=1), "bitwise"))
            out.append((T(lambda s, c=c: f"{s.k('SHIFTRIGHT')}({c.name}, 2)", lambda x, c=c: f"F.shiftright({pyq(c.name)}, 2)", kind="int"),
                        f"{c.name}_high", Column("x", "int", lo=0, hi=3), "bitwise"))
    return out


def rng_zero(st):
    return rng.choice(["0D", "0.0D", "CAST(0 AS DOUBLE)"]) if st.upper else rng.choice(["0D", "0.0D", "cast(0 as double)"])


def stg_nested_select(g: Gen, cur: R, last: bool) -> Out:
    st, ctx = g.st, g.ctx
    taken = set()
    cands = nested_exprs(g, cur, taken)
    if len(cands) < 2:
        skip()
    picked = rng.sample(cands, min(len(cands), rng.choice([2, 3, 4, 5])))
    keep = [c for c in cur.cols if c.kind in PLAIN_KINDS and (c.name in (cur.keys or []) or chance(0.5))]
    taken = set(cur.names())
    items = []
    for e, n, col, f in picked:
        n = fresh(n, taken)
        taken.add(n)
        items.append((e, n, Column(n, col.kind, pool=col.pool, lo=col.lo, hi=col.hi, dtype=col.dtype)))
        g.feats.add(f)
    # keep nested columns so later stages can explode them
    nested_keep = [c for c in cur.cols if c.kind not in PLAIN_KINDS and chance(0.5)]
    conds = []
    rel = R(cur.name, cur.var, keep, cur.keys, cur.base).plain_rel()
    if rel.cols and chance(0.4):
        p, f = G.atom(rel)
        conds.append(p)
    items_sql = [c.name for c in keep] + [c.name for c in nested_keep] + [f"{e.sql(st)} {st.k('AS')} {n}" for e, n, _ in items]
    sql = sql_select(g, items_sql, cur.name, where=[(f"({p.sql(st)})" if isinstance(p, Or) else p.sql(st)) for p in conds])
    steps = []
    if conds:
        steps.append(f".filter({conds[0].py(ctx)})")
    steps.append(call("select", [pyq(c.name) for c in keep + nested_keep] +
                      [(f"{e.pyw(ctx)}.alias({pyq(n)})" if "\n" not in e.py(ctx) else f"{e.py(ctx)}.alias({pyq(n)})") for e, n, _ in items]))
    cols = keep + nested_keep + [c for _, _, c in items]
    keys = cur.keys if cur.keys and all(x in [c.name for c in keep] for x in cur.keys) else None
    g.feats.add("stage_nested")
    return Out(sql, cur.var, steps, cols, keys, hint=choice(["parsed", "unpacked", "flattened_attrs"]),
               comment=choice(["Struct fields, map lookups and JSON extraction", "Higher-order array functions",
                               "Semi-structured parsing"]))


# ---------------------------------------------------------------------------
# Pipelines and families
# ---------------------------------------------------------------------------
def run_pipeline(st: Style, ctx: Ctx, base: Table, stages, feats=None, comments=None):
    feats = feats if feats is not None else set()
    g = Gen(st, ctx, feats, set(TABLE_BY_NAME) | {f"df_{base.name}", "result", "src", "t", "x"})
    keys = ["id"] if any(c.name == "id" for c in base.columns) else None
    cur = R(base.name, f"df_{base.name}", list(base.columns), keys, base)
    ctes, defs = [], []
    main = None
    use_comments = st.multiline and (comments if comments is not None else chance(0.5 if len(stages) < 3 else 0.75))
    for i, stage in enumerate(stages):
        last = i == len(stages) - 1
        out = try_stages(g, cur, last, stage if isinstance(stage, list) else [stage])
        for name, var, sql, py, comment in out.extra:
            ctes.append((name, sql, comment))
            defs.append((var, py))
        if last:
            main = out
        else:
            name = fresh(out.hint if out.hint not in g.names else f"{out.hint}_{base.name}", g.names)
            g.names.add(name)
            ctes.append((name, out.sql, out.comment))
            defs.append((name, chain(out.head, out.steps)))
            cur = R(name, name, out.cols, out.keys, cur.base)
    assert main is not None
    k = st.k
    if ctes:
        blocks = []
        for name, body, comment in ctes:
            if st.multiline:
                inner = "\n".join("    " + line for line in body.split("\n"))
                c = f"    -- {comment}\n" if use_comments and comment else ""
                blocks.append(f"{name} {k('AS')} (\n{c}{inner}\n)")
            else:
                blocks.append(f"{name} {k('AS')} ({body})")
        sep = ",\n" if st.multiline else ", "
        tail = ("\n" if st.multiline else " ")
        if use_comments and main.comment:
            tail = "\n-- " + main.comment + "\n"
        sql = f"{k('WITH')} " + sep.join(blocks) + tail + main.sql
        feats.add("cte")
    else:
        sql = main.sql
    if not defs and not ctx.pre and len(main.steps) == 0:
        skip()
    feats.add("v4")
    feats.add(f"stages_{min(len(ctes) + 1, 6)}")
    return sql, defs, chain(main.head, main.steps), feats, main.order


def try_stages(g: Gen, cur: R, last: bool, options: list) -> Out:
    """Run the first stage (in random order) that applies to `cur`; undo the side effects of the ones that don't."""
    rest = list(options[1:])
    rng.shuffle(rest)
    for stage in [options[0]] + rest:          # the preferred stage first, then the fallbacks
        snap = (list(g.ctx.pre), dict(g.ctx.windows), g.ctx.uses_window, set(g.feats), set(g.names))
        try:
            return stage(g, cur, last)
        except G.Skip:
            g.ctx.pre[:], g.ctx.windows, g.ctx.uses_window = snap[0], snap[1], snap[2]
            g.feats.clear()
            g.feats |= snap[3]
            g.names.clear()
            g.names |= snap[4]
    skip()


def plain_tables():
    return [t for t in train_tables()] + [t for t in extra_train_tables() if all(c.kind in PLAIN_KINDS for c in t.columns)]


def nested_tables():
    return [t for t in extra_train_tables() if any(c.kind not in PLAIN_KINDS for c in t.columns)]


def fam_adv_window(st, ctx):
    t = choice(t for t in plain_tables() if order_candidates(R(t.name, "", list(t.columns), ["id"], t)))
    stages = [stg_windows] if chance(0.5) else ([stg_clean, stg_windows] if chance(0.5) else [stg_windows, stg_final])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_dedup(st, ctx):
    t = choice(plain_tables())
    stages = choice([[stg_dedup], [stg_clean, stg_dedup], [stg_dedup, stg_final], [stg_dedup, stg_agg]])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_agg(st, ctx):
    t = choice(plain_tables())
    stages = choice([[stg_agg], [stg_agg], [stg_clean, stg_agg], [stg_windows, stg_agg]])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_join(st, ctx):
    t = choice(plain_tables())
    kind = choice(["join", "lateral", "anti"])
    first = {"join": stg_join, "lateral": stg_lateral, "anti": stg_anti}[kind]
    stages = choice([[first], [first, stg_final], [stg_clean, first], [first, stg_agg]])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_reshape(st, ctx):
    t = choice(plain_tables())
    stages = choice([[stg_pivot], [stg_unpivot], [stg_clean, stg_pivot], [stg_unpivot, stg_agg], [stg_clean, stg_unpivot]])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_setop(st, ctx):
    t = choice(plain_tables())
    stages = choice([[stg_setop], [stg_clean, stg_setop], [stg_setop, stg_final], [stg_setop, stg_agg]])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_session(st, ctx):
    t = choice(t for t in plain_tables() if any(c.kind == "ts" for c in t.columns))
    stages = choice([[stg_session], [stg_session, stg_agg], [stg_clean, stg_session, stg_agg], [stg_session, stg_final]])
    return run_pipeline(st, ctx, t, stages)


def fam_adv_nested(st, ctx):
    t = choice(nested_tables())
    stages = choice([[stg_nested_select], [stg_explode], [stg_explode, stg_agg], [stg_nested_select, stg_agg],
                     [stg_explode, stg_final], [stg_nested_select, stg_windows], [stg_explode, stg_windows]])
    return run_pipeline(st, ctx, t, stages)


RELATIONAL_MIDDLE = [(stg_windows, 0.25), (stg_dedup, 0.12), (stg_session, 0.12), (stg_join, 0.16), (stg_lateral, 0.08),
                     (stg_anti, 0.1), (stg_setop, 0.07), (stg_clean, 0.1)]
ENDINGS = [(stg_agg, 0.45), (stg_final, 0.3), (stg_pivot, 0.1), (stg_unpivot, 0.07), (stg_windows, 0.08)]


def fam_pipeline(st, ctx):
    """3-6 CTEs chaining several stages, like a production report query."""
    st.multiline = chance(0.92)
    nested = chance(0.3)
    t = choice(nested_tables() if nested else plain_tables())
    n = rng.choice([4, 4, 5, 5, 6, 7, 8])
    stages: list = []
    if nested:
        stages.append([stg_explode, stg_nested_select])
        if chance(0.5):
            stages.append([stg_explode, stg_nested_select])
    elif chance(0.6):
        stages.append([stg_clean])
    middle = [s for s, _ in RELATIONAL_MIDDLE]
    while len(stages) < n - 1:
        first = wchoice(RELATIONAL_MIDDLE)
        stages.append([first] + [s for s in middle if s is not first and s is not stg_setop])
    end = wchoice(ENDINGS)
    stages.append([end] if end in (stg_agg, stg_final) else [end, stg_agg, stg_final])
    if end in (stg_agg, stg_pivot) and chance(0.35):
        stages.append([stg_final])
    return run_pipeline(st, ctx, t, stages)


FAMILIES = [(fam_adv_window, 8), (fam_adv_dedup, 5), (fam_adv_agg, 8), (fam_adv_join, 8), (fam_adv_reshape, 6),
            (fam_adv_setop, 4), (fam_adv_session, 5), (fam_adv_nested, 12), (fam_pipeline, 30)]
