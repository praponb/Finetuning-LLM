"""nested_v20: free, template-built SQL -> PySpark pairs for the harness gaps (q9g round, 2026-11-26).

The harness suites (eval/harness_suite_v2.jsonl, TEST ONLY) show the model failing on nested types, maps, JSON,
higher-order functions, generators, semi/anti joins, set operations, interval window frames, time buckets, gap
filling, DECIMAL typing and NULL semantics. This generator writes pairs for exactly those forms on the 30 TRAINING
domains of schema_v15/synth_catalog.py (never the 5 held-out domains, never the 13 suite tables): every pair gets its
own small typed tables (SELECT CAST(..) ... FROM VALUES), the SQL and the PySpark are written side by side, and
verify.py keeps only pairs whose results are identical type-strict (sql2pyspark_harness/compare_harness.compare_one)
and in ORDER BY order.

  .venv/bin/python nested_v20/generate.py --n 6000 --seed 20 --out nested_v20/raw/cands.jsonl
"""
from __future__ import annotations

import argparse
import json
import random
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
sys.path.insert(0, str(MLX))
sys.path.insert(0, str(MLX / "schema_v15"))
import synth_catalog as sc  # noqa: E402

SUITE_TABLES = {"customers", "products", "orders", "order_items", "payments", "fx_rates", "web_events", "promotions",
                "inventory_movements", "tier_history", "category_tree", "support_tickets", "price_history"}
TRAIN_DOMAINS = [d for d in sc.DOMAINS if d not in sc.HELDOUT_DOMAINS]


def singular(name: str) -> str:
    for end in ("ches", "shes", "sses", "xes"):
        if name.endswith(end):
            return name[:-2]
    return sc.singular(name)


# ---------------------------------------------------------------------------------------------------------------
# tables
# ---------------------------------------------------------------------------------------------------------------
def lit(v, t: str) -> str:
    """Spark SQL literal of a python value for declared type t (the table SQL casts every column to t anyway)."""
    if v is None:
        return "NULL"
    if t == "STRING":
        return "'" + str(v).replace("'", "''") + "'"
    if t == "BOOLEAN":
        return "TRUE" if v else "FALSE"
    if t == "INT" or t == "BIGINT":
        return str(int(v))
    if t == "DOUBLE":
        return f"{float(v)}D"
    if t.startswith("DECIMAL"):
        return f"CAST({v} AS {t})"
    if t == "DATE":
        return f"DATE '{v.isoformat()}'"
    if t == "TIMESTAMP":
        return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S')}'"
    if t.startswith("ARRAY<STRUCT"):
        inner = t[len("ARRAY<"):-1]
        return f"CAST(ARRAY({', '.join(lit(x, inner) for x in v)}) AS {t})" if v else f"CAST(ARRAY() AS {t})"
    if t.startswith("ARRAY<"):
        inner = t[len("ARRAY<"):-1]
        return f"CAST(ARRAY({', '.join(lit(x, inner) for x in v)}) AS {t})" if v else f"CAST(ARRAY() AS {t})"
    if t.startswith("STRUCT<"):
        fields = _struct_fields(t)
        return "NAMED_STRUCT(" + ", ".join(f"'{n}', {lit(v[n], ft)}" for n, ft in fields) + ")"
    if t.startswith("MAP<"):
        if not v:
            return f"CAST(MAP() AS {t})"
        vt = t[len("MAP<STRING,"):-1].strip()
        return f"CAST(MAP({', '.join(lit(k, 'STRING') + ', ' + lit(x, vt) for k, x in v.items())}) AS {t})"
    raise ValueError(t)


def _struct_fields(t: str) -> list[tuple[str, str]]:
    body = t[len("STRUCT<"):-1]
    out, depth, cur = [], 0, ""
    for ch in body:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += ch
    out.append(cur)
    res = []
    for f in out:
        n, ft = f.strip().split(":", 1)
        res.append((n.strip(), ft.strip()))
    return res


class Tbl:
    def __init__(self, name: str):
        self.name, self.cols, self.rows = name, [], []

    def add(self, name: str, typ: str, role: str):
        self.cols.append((name, typ, role))

    def col(self, role: str) -> str:
        return next(n for n, _, r in self.cols if r == role)

    def has(self, role: str) -> bool:
        return any(r == role for _, _, r in self.cols)

    def typ(self, name: str) -> str:
        return next(t for n, t, _ in self.cols if n == name)

    def sql(self) -> str:
        names = [n for n, _, _ in self.cols]
        sel = ", ".join(f"CAST({n} AS {t}) AS {n}" for n, t, _ in self.cols)
        vals = ",\n  ".join("(" + ", ".join(lit(r[n], t) for n, t, _ in self.cols) + ")" for r in self.rows)
        return f"SELECT {sel} FROM VALUES\n  {vals}\n  AS t({', '.join(names)})"


def fields_of(spec: str) -> list[tuple]:
    return sc._fields(spec)


def fval(rng, kind, args):
    if kind == "cat":
        return rng.choice(args)
    if kind == "int":
        return rng.randint(int(args[0]), int(args[1]))
    if kind == "double":
        return round(rng.uniform(args[0], args[1]), 2)
    return rng.random() < 0.5


FTYPE = {"cat": "STRING", "int": "INT", "double": "DOUBLE", "bool": "BOOLEAN"}


class Ctx:
    """One pair's world: a nested main table T and a child table C from one training domain."""

    def __init__(self, rng: random.Random, domain: str):
        self.rng, self.domain = rng, domain
        dom = sc.DOMAINS[domain]
        tnames = [t for t, _ in dom["tables"] if t not in SUITE_TABLES]
        main, child = tnames[0], rng.choice(tnames[1:])
        self.main_base = main
        arr, arr_spec, st, st_spec = dom["nest"]
        self.af, self.sf = fields_of(arr_spec), fields_of(st_spec)
        self.arr, self.st = arr, st
        cats = sorted(dom["cat"])
        nums = sorted(dom["num"])
        rng.shuffle(cats)
        rng.shuffle(nums)
        ints = [n for n in nums if dom["num"][n][2] == "int"]
        dbls = [n for n in nums if dom["num"][n][2] == "double"]
        while len(ints) < 2:
            ints.append(f"{rng.choice(['unit', 'item', 'event'])}_cnt_{len(ints) + 1}")
        while len(dbls) < 2:
            dbls.append(f"{rng.choice(['score', 'ratio', 'level'])}_val_{len(dbls) + 1}")
        self.cat1, self.cat2 = cats[0], cats[1 % len(cats)] if len(cats) > 1 else cats[0] + "_alt"
        self.catv = {self.cat1: list(dom["cat"][cats[0]]), self.cat2: list(dom["cat"].get(self.cat2, ("A", "B", "C")))}
        self.i1, self.i2 = ints[0], ints[1]
        self.d1, self.d2 = dbls[0], dbls[1]
        rng_int = {n: dom["num"].get(n, (1, 50, "int"))[:2] for n in (self.i1, self.i2)}
        rng_dbl = {n: dom["num"].get(n, (1.0, 99.0, "double"))[:2] for n in (self.d1, self.d2)}
        base_amt = [n for n in nums if any(k in n for k in ("cost", "fee", "usd", "price", "amount", "amt", "rent",
                                                             "salary", "pay", "value", "budget"))]
        self.amt = (base_amt[0] + "_dec") if base_amt else rng.choice(["amount_due", "net_amount", "billed_amt"])
        self.dt = rng.choice(dom["dt"])
        self.ts = rng.choice(dom["ts"])
        self.flag = rng.choice(dom["flag"])
        self.label = rng.choice(sorted(dom["txt"]))
        labels = list(dom["txt"][self.label])
        self.m = rng.choice([f"{self.cat1}_counts", f"{self.cat2}_hits", f"{singular(main)}_scores"])
        self.mkeys = list(dom["cat"][cats[0]])
        self.js = rng.choice(["payload", "attrs_json", f"{singular(main)}_json", "meta"])
        self.jf = [(n, k, a) for n, k, a in self.af][:2]           # top-level json fields
        self.jn = self.st                                          # nested json object name
        self.jnf = self.sf[:2]
        self.xs = rng.choice([f"{self.i1}_series", f"daily_{self.i1}", f"{self.i1}_hist"])
        tname = rng.choice([f"{singular(main)}_{arr}", f"{main}_detail", f"{singular(main)}_{arr}_log"])
        self.T = T = Tbl(tname if tname not in SUITE_TABLES else tname + "_x")
        T.add("id", "INT", "id")
        T.add(self.cat1, "STRING", "cat1")
        T.add(self.cat2, "STRING", "cat2")
        T.add(self.label, "STRING", "label")
        T.add(self.i1, "INT", "i1")
        T.add(self.i2, "INT", "i2")
        T.add(self.d1, "DOUBLE", "d1")
        T.add(self.d2, "DOUBLE", "d2")
        T.add(self.amt, "DECIMAL(12,2)", "amt")
        T.add(self.dt, "DATE", "dt")
        T.add(self.ts, "TIMESTAMP", "ts")
        T.add(self.flag, "BOOLEAN", "flag")
        st_t = "STRUCT<" + ", ".join(f"{n}: {FTYPE[k]}" for n, k, _ in self.af) + ">"
        T.add(arr, f"ARRAY<{st_t}>", "arr")
        T.add(st, "STRUCT<" + ", ".join(f"{n}: {FTYPE[k]}" for n, k, _ in self.sf) + ">", "st")
        T.add(self.m, "MAP<STRING,INT>", "map")
        T.add(self.js, "STRING", "js")
        T.add(self.xs, "ARRAY<INT>", "xs")
        n = rng.randint(9, 14)
        i1s = rng.sample(range(int(rng_int[self.i1][0]), int(rng_int[self.i1][0]) + 400), n)
        d0 = date(2025, rng.randint(1, 10), rng.randint(1, 18))
        t0 = datetime(2025, rng.randint(1, 10), rng.randint(1, 20), rng.randint(6, 10), 0, 0)
        dts = sorted(rng.sample(range(0, 16), min(n, 12)) + rng.sample(range(0, 16), max(0, n - 12)))
        tmin = sorted(rng.sample(range(0, 60 * 30, 7), n))
        for i in range(n):
            r = {"id": i + 1,
                 self.cat1: rng.choice(self.catv[self.cat1]),
                 self.cat2: None if rng.random() < 0.15 else rng.choice(self.catv[self.cat2]),
                 self.label: rng.choice(labels),
                 self.i1: None if rng.random() < 0.08 else i1s[i],
                 self.i2: None if rng.random() < 0.2 else rng.randint(int(rng_int[self.i2][0]), int(rng_int[self.i2][1])),
                 self.d1: round(rng.uniform(*rng_dbl[self.d1]), 2),
                 self.d2: None if rng.random() < 0.15 else round(rng.uniform(*rng_dbl[self.d2]), 2),
                 self.amt: None if rng.random() < 0.1 else f"{rng.uniform(5, 950):.2f}",
                 self.dt: None if rng.random() < 0.08 else d0 + timedelta(days=dts[i]),
                 self.ts: t0 + timedelta(minutes=tmin[i]),
                 self.flag: rng.random() < 0.5,
                 arr: [{fn: fval(rng, k, a) for fn, k, a in self.af} for _ in range(rng.choice([0, 1, 2, 2, 3, 4]))],
                 st: None if rng.random() < 0.08 else {fn: fval(rng, k, a) for fn, k, a in self.sf},
                 self.m: None if rng.random() < 0.08 else {k: rng.randint(0, 20) for k in rng.sample(self.mkeys, rng.randint(0, min(3, len(self.mkeys))))},
                 self.js: None if rng.random() < 0.08 else json.dumps(self._json_obj(rng)),
                 self.xs: None if rng.random() < 0.08 else [rng.randint(0, 40) for _ in range(rng.choice([0, 2, 3, 4, 5]))]}
            T.rows.append(r)
        cname = child if child not in SUITE_TABLES else child + "_x"
        self.C = C = Tbl(cname)
        self.fk = f"{singular(tname)}_id" if rng.random() < 0.7 else f"{singular(tname)}_ref"
        self.ccat = rng.choice([self.cat1, f"{self.cat1}_seen", f"src_{self.cat1}"])
        self.cnum = rng.choice([n for n in nums if n not in (self.i1, self.i2)] or ["qty_logged"])
        self.cts = rng.choice([t for t in dom["ts"]] + [f"logged_at"])
        C.add("id", "INT", "id")
        C.add(self.fk, "INT", "fk")
        C.add(self.ccat, "STRING", "ccat")
        C.add(self.cnum, "INT", "cnum")
        C.add(self.cts, "TIMESTAMP", "cts")
        m = rng.randint(8, 14)
        for j in range(m):
            C.rows.append({"id": j + 1, self.fk: None if rng.random() < 0.1 else rng.randint(1, n + 2),
                           self.ccat: rng.choice(self.catv[self.cat1]),
                           self.cnum: None if rng.random() < 0.1 else rng.randint(0, 60),
                           self.cts: t0 + timedelta(minutes=rng.randint(0, 60 * 30))})
        self.th_i1 = sorted(i1s)[n // 2]
        self.up = rng.random() < 0.6

    def _json_obj(self, rng):
        o = {}
        for fn, k, a in self.jf:
            if rng.random() < 0.9:
                o[fn] = fval(rng, k, a)
        o[self.jn] = {fn: fval(rng, k, a) for fn, k, a in self.jnf}
        return o

    def kw(self, s: str) -> str:
        return s if self.up else s.lower()


def q(s: str) -> str:
    return json.dumps(s)


def low_kw(sql: str) -> str:
    """lowercase SQL keywords / function names outside quoted strings (style knob)"""
    parts = re.split(r"('(?:[^']|'')*')", sql)
    return "".join(p if i % 2 else re.sub(r"\b[A-Z][A-Z_]+\b", lambda m: m.group(0).lower(), p)
                   for i, p in enumerate(parts))


IMP = "from pyspark.sql import functions as F\n"
IMPW = "from pyspark.sql import functions as F\nfrom pyspark.sql.window import Window\n"


# ---------------------------------------------------------------------------------------------------------------
# families: each returns (sql, code, order_cols)
# ---------------------------------------------------------------------------------------------------------------
def fam_posexplode(c: Ctx):
    T, r = c.T, c.rng
    outer = r.random() < 0.5
    f1, f2 = c.af[0][0], c.af[1][0]
    nf = next((n for n, k, _ in c.af if k in ("int", "double")), f2)
    th = fval(r, *next((k, a) for n, k, a in c.af if n == nf)) if nf != f2 or c.af[1][1] in ("int", "double") else None
    pos = f"{singular(c.arr)}_pos" if r.random() < 0.6 else "slot"
    where_sql, where_py = "", ""
    if th is not None and r.random() < 0.6:
        th_l = th if isinstance(th, (int, float)) else None
        if th_l is not None:
            if outer:
                where_sql = f"\nWHERE e.{nf} IS NULL OR e.{nf} >= {th_l}"
                where_py = f'\n    .filter(F.col("e.{nf}").isNull() | (F.col("e.{nf}") >= {th_l}))'
            else:
                where_sql = f"\nWHERE e.{nf} >= {th_l}"
                where_py = f'\n    .filter(F.col("e.{nf}") >= {th_l})'
    sql = (f"SELECT id, {c.cat1}, pos AS {pos}, e.{f1}, e.{f2}\nFROM {T.name}\n"
           f"LATERAL VIEW {'OUTER ' if outer else ''}POSEXPLODE({c.arr}) x AS pos, e{where_sql}\nORDER BY id, {pos}")
    fn = "posexplode_outer" if outer else "posexplode"
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\"id\", \"{c.cat1}\", F.{fn}(\"{c.arr}\").alias(\"pos\", \"e\")){where_py}\n"
            f"    .select(\n        \"id\",\n        \"{c.cat1}\",\n        F.col(\"pos\").alias(\"{pos}\"),\n"
            f"        F.col(\"e.{f1}\"),\n        F.col(\"e.{f2}\"),\n    )\n    .orderBy(\"id\", \"{pos}\")\n)")
    return sql, code, ["id", pos]


def fam_inline(c: Ctx):
    T, r = c.T, c.rng
    outer = r.random() < 0.5
    fn = "INLINE_OUTER" if outer else "INLINE"
    nums = [(n, k) for n, k, _ in c.af if k in ("int", "double")]
    cats = [n for n, k, _ in c.af if k == "cat"]
    agg_sql, agg_py = [f"COUNT({c.af[0][0]}) AS n_{c.arr}"], [f'F.count("{c.af[0][0]}").alias("n_{c.arr}")']
    for n, k in nums[:2]:
        if r.random() < 0.5:
            agg_sql.append(f"ROUND(AVG({n}), 2) AS avg_{n}")
            agg_py.append(f'F.round(F.avg("{n}"), 2).alias("avg_{n}")')
        else:
            agg_sql.append(f"MAX({n}) AS max_{n}")
            agg_py.append(f'F.max("{n}").alias("max_{n}")')
    if cats and r.random() < 0.6:
        agg_sql.append(f"COUNT(DISTINCT {cats[0]}) AS distinct_{cats[0]}")
        agg_py.append(f'F.countDistinct("{cats[0]}").alias("distinct_{cats[0]}")')
    cte = f"{singular(c.arr)}_rows"
    sql = (f"WITH {cte} AS (\n    SELECT id, {c.cat1}, {fn}({c.arr})\n    FROM {T.name}\n)\n"
           f"SELECT {c.cat1},\n       " + ",\n       ".join(agg_sql) +
           f"\nFROM {cte}\nGROUP BY {c.cat1}\nORDER BY {c.cat1}")
    code = (IMP + f"\n{cte} = df_{T.name}.select(\"id\", \"{c.cat1}\", F.{fn.lower()}(\"{c.arr}\"))\n\n"
            f"result = (\n    {cte}\n    .groupBy(\"{c.cat1}\")\n    .agg(\n        " + ",\n        ".join(agg_py) +
            f",\n    )\n    .orderBy(\"{c.cat1}\")\n)")
    return sql, code, [c.cat1]


def fam_map(c: Ctx):
    T, r = c.T, c.rng
    k = r.choice(c.mkeys)
    th = r.randint(3, 12)
    kk = re.sub(r"\W", "_", k.lower())
    if r.random() < 0.55:
        sel_sql = [f"SIZE({c.m}) AS n_keys", f"TRY_ELEMENT_AT({c.m}, '{k}') AS {kk}_val",
                   f"ARRAY_SORT(MAP_KEYS(MAP_FILTER({c.m}, (k, v) -> v >= {th}))) AS keys_over_{th}",
                   f"TRANSFORM_VALUES({c.m}, (k, v) -> v * 10) AS {c.m}_x10"]
        sel_py = [f'F.size("{c.m}").alias("n_keys")', f'F.try_element_at("{c.m}", F.lit({q(k)})).alias("{kk}_val")',
                  f'F.array_sort(F.map_keys(F.map_filter("{c.m}", lambda k, v: v >= {th}))).alias("keys_over_{th}")',
                  f'F.transform_values("{c.m}", lambda k, v: v * 10).alias("{c.m}_x10")']
        if r.random() < 0.5:
            sel_sql.append(f"AGGREGATE(MAP_VALUES({c.m}), 0, (acc, v) -> acc + v) AS total_{c.m}")
            sel_py.append(f'F.aggregate(F.map_values("{c.m}"), F.lit(0), lambda acc, v: acc + v).alias("total_{c.m}")')
        sql = (f"SELECT id, {c.cat1},\n       " + ",\n       ".join(sel_sql) +
               f"\nFROM {T.name}\nWHERE {c.m} IS NOT NULL\nORDER BY id")
        code = (IMP + f"\nresult = (\n    df_{T.name}\n    .filter(F.col(\"{c.m}\").isNotNull())\n    .select(\n"
                f"        \"id\",\n        \"{c.cat1}\",\n        " + ",\n        ".join(sel_py) +
                ",\n    )\n    .orderBy(\"id\")\n)")
        return sql, code, ["id"]
    key = r.choice(["key_name", f"{c.cat1}_key", "map_key"])
    val = r.choice(["hits", "cnt", f"{c.m}_value"])
    sql = (f"SELECT {c.cat1}, {key}, SUM({val}) AS total_{val}, COUNT(*) AS n_rows\nFROM {T.name}\n"
           f"LATERAL VIEW EXPLODE({c.m}) x AS {key}, {val}\nGROUP BY {c.cat1}, {key}\nORDER BY {c.cat1}, {key}")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\"{c.cat1}\", F.explode(\"{c.m}\").alias(\"{key}\", \"{val}\"))\n"
            f"    .groupBy(\"{c.cat1}\", \"{key}\")\n    .agg(\n        F.sum(\"{val}\").alias(\"total_{val}\"),\n"
            f"        F.count(F.lit(1)).alias(\"n_rows\"),\n    )\n    .orderBy(\"{c.cat1}\", \"{key}\")\n)")
    return sql, code, [c.cat1, key]


def fam_json(c: Ctx):
    T, r = c.T, c.rng
    schema = ", ".join(f"{n} {FTYPE[k]}" for n, k, _ in c.jf)
    schema += f", {c.jn} STRUCT<" + ", ".join(f"{n}: {FTYPE[k]}" for n, k, _ in c.jnf) + ">"
    (a, ak, _), (b, bk, _) = c.jf[0], c.jf[1]
    nf, nk = c.jnf[0][0], c.jnf[0][1]
    cte = r.choice(["parsed", f"{c.js}_parsed", "decoded"])
    filt_sql, filt_py = "", ""
    numf = next((n for n, k, _ in c.jf if k in ("int", "double")), None)
    if numf and r.random() < 0.6:
        th = fval(r, *next((k, x) for n, k, x in c.jf if n == numf))
        filt_sql = f"\nWHERE p.{numf} >= {th}"
        filt_py = f'\n    .filter(F.col("p.{numf}") >= {th})'
    sql = (f"WITH {cte} AS (\n    SELECT id, {c.cat1},\n           FROM_JSON({c.js}, '{schema}') AS p,\n"
           f"           GET_JSON_OBJECT({c.js}, '$.{c.jn}.{nf}') AS {nf}_raw\n    FROM {T.name}\n)\n"
           f"SELECT id, {c.cat1}, p.{a} AS {a}, p.{b} AS {b}, p.{c.jn}.{nf} AS {c.jn}_{nf}, {nf}_raw\n"
           f"FROM {cte}{filt_sql}\nORDER BY id")
    code = (IMP + f"\n{cte} = df_{T.name}.select(\n    \"id\",\n    \"{c.cat1}\",\n"
            f"    F.from_json(\"{c.js}\", \"{schema}\").alias(\"p\"),\n"
            f"    F.get_json_object(\"{c.js}\", \"$.{c.jn}.{nf}\").alias(\"{nf}_raw\"),\n)\n\n"
            f"result = (\n    {cte}{filt_py}\n    .select(\n        \"id\",\n        \"{c.cat1}\",\n"
            f"        F.col(\"p.{a}\").alias(\"{a}\"),\n        F.col(\"p.{b}\").alias(\"{b}\"),\n"
            f"        F.col(\"p.{c.jn}.{nf}\").alias(\"{c.jn}_{nf}\"),\n        \"{nf}_raw\",\n    )\n    .orderBy(\"id\")\n)")
    return sql, code, ["id"]


def fam_hof(c: Ctx):
    T, r = c.T, c.rng
    th = r.randint(15, 35)
    sel_sql, sel_py = [], []
    opts = [
        (f"TRANSFORM({c.xs}, (x, i) -> x * (i + 1)) AS weighted_{c.xs}",
         f'F.transform("{c.xs}", lambda x, i: x * (i + 1)).alias("weighted_{c.xs}")'),
        (f"FILTER({c.xs}, (x, i) -> i % 2 = 0) AS even_slots",
         f'F.filter("{c.xs}", lambda x, i: i % 2 == 0).alias("even_slots")'),
        (f"AGGREGATE({c.xs}, 0, (acc, x) -> acc + x) AS total_{c.i1}",
         f'F.aggregate("{c.xs}", F.lit(0), lambda acc, x: acc + x).alias("total_{c.i1}")'),
        (f"AGGREGATE({c.xs}, CAST(0 AS DOUBLE), (acc, x) -> acc + x, acc -> ROUND(acc / GREATEST(SIZE({c.xs}), 1), 2)) AS mean_{c.i1}",
         f'F.aggregate(\n            "{c.xs}",\n            F.lit(0.0),\n            lambda acc, x: acc + x,\n'
         f'            lambda acc: F.round(acc / F.greatest(F.size("{c.xs}"), F.lit(1)), 2),\n        ).alias("mean_{c.i1}")'),
        (f"EXISTS({c.xs}, x -> x > {th}) AS any_over_{th}",
         f'F.exists("{c.xs}", lambda x: x > {th}).alias("any_over_{th}")'),
        (f"FORALL({c.xs}, x -> x >= 5) AS all_at_least_5",
         f'F.forall("{c.xs}", lambda x: x >= 5).alias("all_at_least_5")'),
    ]
    numf = [(n, k, a) for n, k, a in c.af if k in ("int", "double")]
    catf = [(n, k, a) for n, k, a in c.af if k == "cat"]
    if numf:
        n, k, a = numf[0]
        tv = fval(r, k, a)
        opts.append((f"FILTER({c.arr}, e -> e.{n} >= {tv}) AS {c.arr}_high",
                     f'F.filter("{c.arr}", lambda e: e["{n}"] >= {tv}).alias("{c.arr}_high")'))
        opts.append((f"TRANSFORM({c.arr}, e -> e.{n}) AS {n}_list",
                     f'F.transform("{c.arr}", lambda e: e["{n}"]).alias("{n}_list")'))
    if catf:
        n, k, a = catf[0]
        v = r.choice(a)
        opts.append((f"EXISTS({c.arr}, e -> e.{n} = '{v}') AS has_{re.sub(r'W', '_', v.lower())}".replace("W", "\\W"),
                     None))
        opts[-1] = (f"EXISTS({c.arr}, e -> e.{n} = '{v}') AS has_{re.sub(r'[^a-z0-9]+', '_', v.lower())}",
                    f'F.exists("{c.arr}", lambda e: e["{n}"] == {q(v)}).alias("has_{re.sub(r"[^a-z0-9]+", "_", v.lower())}")')
    for s, p in r.sample(opts, r.randint(3, 5)):
        sel_sql.append(s)
        sel_py.append(p)
    sql = f"SELECT id, {c.cat1},\n       " + ",\n       ".join(sel_sql) + f"\nFROM {T.name}\nORDER BY id"
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\n        \"id\",\n        \"{c.cat1}\",\n        " +
            ",\n        ".join(sel_py) + ",\n    )\n    .orderBy(\"id\")\n)")
    return sql, code, ["id"]


def fam_array_fns(c: Ctx):
    T, r = c.T, c.rng
    numf = [n for n, k, _ in c.af if k in ("int", "double")]
    opts = [
        (f"SLICE({c.xs}, 1, 2) AS first_two", f'F.slice("{c.xs}", 1, 2).alias("first_two")'),
        (f"TRY_ELEMENT_AT({c.xs}, -1) AS last_{c.i1}", f'F.try_element_at("{c.xs}", F.lit(-1)).alias("last_{c.i1}")'),
        (f"TRY_ELEMENT_AT({c.xs}, 3) AS third_{c.i1}", f'F.try_element_at("{c.xs}", F.lit(3)).alias("third_{c.i1}")'),
        (f"ARRAY_POSITION({c.xs}, 10) AS pos_of_10", f'F.array_position("{c.xs}", 10).alias("pos_of_10")'),
        (f"ARRAY_MAX({c.xs}) AS peak_{c.i1}", f'F.array_max("{c.xs}").alias("peak_{c.i1}")'),
        (f"ARRAY_DISTINCT({c.xs}) AS distinct_{c.i1}", f'F.array_distinct("{c.xs}").alias("distinct_{c.i1}")'),
        (f"SORT_ARRAY({c.xs}, FALSE) AS {c.i1}_desc", f'F.sort_array("{c.xs}", asc=False).alias("{c.i1}_desc")'),
    ]
    if numf:
        n = numf[0]
        opts.append((f"ARRAY_SORT({c.arr}, (l, r) -> CASE WHEN l.{n} > r.{n} THEN -1 WHEN l.{n} < r.{n} THEN 1 ELSE 0 END) AS {c.arr}_by_{n}",
                     f'F.array_sort(\n            "{c.arr}",\n            lambda l, r: F.when(l["{n}"] > r["{n}"], -1).when(l["{n}"] < r["{n}"], 1).otherwise(0),\n        ).alias("{c.arr}_by_{n}")'))
        opts.append((f"ARRAYS_ZIP({c.xs}, TRANSFORM({c.arr}, e -> e.{n})) AS zipped",
                     f'F.arrays_zip(F.col("{c.xs}"), F.transform("{c.arr}", lambda e: e["{n}"])).alias("zipped")'))
    picks = r.sample(opts, r.randint(3, 5))
    sql = (f"SELECT id,\n       " + ",\n       ".join(s for s, _ in picks) + f"\nFROM {T.name}\nORDER BY id")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\n        \"id\",\n        " +
            ",\n        ".join(p for _, p in picks) + ",\n    )\n    .orderBy(\"id\")\n)")
    return sql, code, ["id"]


def fam_by_agg(c: Ctx):
    T, r = c.T, c.rng
    th = c.th_i1
    opts = [
        (f"MAX_BY({c.label}, {c.i1}) AS top_{c.label}", f'F.max_by("{c.label}", "{c.i1}").alias("top_{c.label}")'),
        (f"MIN_BY(id, {c.ts}) AS first_id", f'F.min_by("id", "{c.ts}").alias("first_id")'),
        (f"COUNT_IF({c.flag}) AS n_{c.flag}", f'F.count_if(F.col("{c.flag}")).alias("n_{c.flag}")'),
        (f"BOOL_OR({c.i1} > {th}) AS any_over_{th}", f'F.bool_or(F.col("{c.i1}") > {th}).alias("any_over_{th}")'),
        (f"BOOL_AND({c.flag}) AS all_{c.flag}", f'F.bool_and(F.col("{c.flag}")).alias("all_{c.flag}")'),
        (f"COUNT_IF({c.i2} IS NULL) AS missing_{c.i2}", f'F.count_if(F.col("{c.i2}").isNull()).alias("missing_{c.i2}")'),
        (f"MAX_BY(id, {c.ts}) AS last_id", f'F.max_by("id", "{c.ts}").alias("last_id")'),
    ]
    picks = r.sample(opts, r.randint(3, 5))
    sql = (f"SELECT {c.cat1},\n       COUNT(*) AS n_rows,\n       " + ",\n       ".join(s for s, _ in picks) +
           f"\nFROM {T.name}\nGROUP BY {c.cat1}\nORDER BY {c.cat1}")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .groupBy(\"{c.cat1}\")\n    .agg(\n        F.count(F.lit(1)).alias(\"n_rows\"),\n        " +
            ",\n        ".join(p for _, p in picks) + f",\n    )\n    .orderBy(\"{c.cat1}\")\n)")
    return sql, code, [c.cat1]


def fam_semi_anti(c: Ctx):
    T, C, r = c.T, c.C, c.rng
    th = r.randint(5, 40)
    kind = r.choice(["semi", "anti", "in", "not_exists", "not_in"])
    cols = ["id", c.cat1, r.choice([c.i1, c.d1, c.amt])]
    sel_sql = ", ".join(f"p.{x}" for x in cols)
    sel_py = ",\n        ".join(f'F.col("p.{x}")' for x in cols)
    cond_sql = f"c.{c.fk} = p.id AND c.{c.cnum} >= {th}"
    cond_py = f'(F.col("c.{c.fk}") == F.col("p.id")) & (F.col("c.{c.cnum}") >= {th})'
    how = "left_semi" if kind in ("semi", "in") else "left_anti"
    if kind == "semi":
        body = f"FROM {T.name} p\nLEFT SEMI JOIN {C.name} c\n  ON {cond_sql}"
    elif kind == "anti":
        body = f"FROM {T.name} p\nLEFT ANTI JOIN {C.name} c\n  ON {cond_sql}"
    elif kind == "in":
        body = f"FROM {T.name} p\nWHERE p.id IN (SELECT c.{c.fk} FROM {C.name} c WHERE c.{c.cnum} >= {th})"
        cond_py = f'(F.col("c.{c.fk}") == F.col("p.id"))'
    elif kind == "not_exists":
        body = f"FROM {T.name} p\nWHERE NOT EXISTS (\n    SELECT 1 FROM {C.name} c\n    WHERE {cond_sql}\n)"
    else:
        body = (f"FROM {T.name} p\nWHERE p.id NOT IN (\n    SELECT c.{c.fk} FROM {C.name} c\n"
                f"    WHERE c.{c.fk} IS NOT NULL AND c.{c.cnum} >= {th}\n)")
    sql = f"SELECT {sel_sql}\n{body}\nORDER BY p.id"
    if kind == "in":
        sub = f'df_{C.name}.filter(F.col("{c.cnum}") >= {th}).alias("c")'
    elif kind == "not_in":
        sub = f'df_{C.name}.filter(F.col("{c.fk}").isNotNull() & (F.col("{c.cnum}") >= {th})).alias("c")'
        cond_py = f'(F.col("c.{c.fk}") == F.col("p.id"))'
    else:
        sub = f'df_{C.name}.alias("c")'
    code = (IMP + f"\nresult = (\n    df_{T.name}.alias(\"p\")\n    .join(\n        {sub},\n        {cond_py},\n        \"{how}\",\n    )\n"
            f"    .select(\n        {sel_py},\n    )\n    .orderBy(\"id\")\n)")
    return sql, code, ["id"]


def fam_setop(c: Ctx):
    T, C, r = c.T, c.C, c.rng
    op = r.choice(["INTERSECT ALL", "EXCEPT ALL", "INTERSECT", "EXCEPT", "UNION"])
    meth = {"INTERSECT ALL": "intersectAll", "EXCEPT ALL": "exceptAll", "INTERSECT": "intersect",
            "EXCEPT": "subtract", "UNION": "union"}[op]
    th = r.randint(5, 40)
    a_sql = f"SELECT {c.cat1} FROM {T.name} WHERE {c.flag}"
    a_py = f'df_{T.name}.filter(F.col("{c.flag}")).select("{c.cat1}")'
    b_sql = f"SELECT {c.ccat} FROM {C.name} WHERE {c.cnum} >= {th}"
    b_py = f'df_{C.name}.filter(F.col("{c.cnum}") >= {th}).select(F.col("{c.ccat}").alias("{c.cat1}"))'
    if op == "UNION":
        sql = f"{a_sql}\nUNION\n{b_sql}\nORDER BY {c.cat1}"
        code = (IMP + f"\nleft_side = {a_py}\nright_side = {b_py}\n\nresult = left_side.union(right_side).distinct().orderBy(\"{c.cat1}\")")
    else:
        sql = f"{a_sql}\n{op}\n{b_sql}\nORDER BY {c.cat1}"
        code = (IMP + f"\nleft_side = {a_py}\nright_side = {b_py}\n\nresult = left_side.{meth}(right_side).orderBy(\"{c.cat1}\")")
    return sql, code, [c.cat1]


def fam_range_frame(c: Ctx):
    T, r = c.T, c.rng
    if r.random() < 0.6:
        h = r.choice([1, 2, 3, 6])
        name = f"last_{h}h"
        frame_sql = f"ORDER BY {c.ts} RANGE BETWEEN INTERVAL '{h}' HOUR PRECEDING AND CURRENT ROW"
        w_py = (f'Window.partitionBy("{c.cat1}").orderBy(F.col("{c.ts}").cast("long")).rangeBetween(-{h * 3600}, 0)')
        order = [c.cat1, c.ts, "id"]
        base_cols = ["id", c.cat1, c.ts, c.i1]
        filt_sql, filt_py = "", ""
    else:
        dd = r.choice([2, 3, 7])
        name = f"last_{dd}d"
        frame_sql = f"ORDER BY {c.dt} RANGE BETWEEN INTERVAL '{dd}' DAY PRECEDING AND CURRENT ROW"
        w_py = f'Window.partitionBy("{c.cat1}").orderBy(F.unix_date("{c.dt}")).rangeBetween(-{dd}, 0)'
        order = [c.cat1, c.dt, "id"]
        base_cols = ["id", c.cat1, c.dt, c.i1]
        filt_sql, filt_py = f"\nWHERE {c.dt} IS NOT NULL", f'\n    .filter(F.col("{c.dt}").isNotNull())'
    agg = r.choice([("SUM", "sum"), ("COUNT", "count"), ("MAX", "max")])
    a_sql = f"{agg[0]}({c.i1}) OVER (PARTITION BY {c.cat1} {frame_sql}) AS {c.i1}_{agg[1]}_{name}"
    a_py = f'F.{agg[1]}("{c.i1}").over(w_{name}).alias("{c.i1}_{agg[1]}_{name}")'
    sql = (f"SELECT {', '.join(base_cols)},\n       {a_sql}\nFROM {T.name}{filt_sql}\nORDER BY {', '.join(order)}")
    code = (IMPW + f"\nw_{name} = {w_py}\n\nresult = (\n    df_{T.name}{filt_py}\n    .select(\n        " +
            ",\n        ".join(q(x) for x in base_cols) + f",\n        {a_py},\n    )\n    .orderBy({', '.join(q(x) for x in order)})\n)")
    return sql, code, order


def fam_unpivot(c: Ctx):
    T, r = c.T, c.rng
    mode = r.choice(["unpivot", "unpivot_incl", "stack", "unpivot2"])
    metric, val = r.choice([("metric", "value"), ("measure", "amount"), ("kpi", "reading")])
    if mode in ("unpivot", "unpivot_incl"):
        incl = mode == "unpivot_incl"
        sql = (f"SELECT id, {c.cat1}, {metric}, {val}\nFROM (\n    SELECT id, {c.cat1}, {c.d1}, {c.d2}\n    FROM {T.name}\n)\n"
               f"UNPIVOT {'INCLUDE NULLS ' if incl else ''}({val} FOR {metric} IN ({c.d1}, {c.d2}))\nORDER BY id, {metric}")
        filt = "" if incl else f'\n    .filter(F.col("{val}").isNotNull())'
        code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\"id\", \"{c.cat1}\", \"{c.d1}\", \"{c.d2}\")\n"
                f"    .unpivot([\"id\", \"{c.cat1}\"], [\"{c.d1}\", \"{c.d2}\"], \"{metric}\", \"{val}\"){filt}\n"
                f"    .orderBy(\"id\", \"{metric}\")\n)")
        return sql, code, ["id", metric]
    if mode == "stack":
        sql = (f"SELECT id, STACK(2, '{c.d1}', {c.d1}, '{c.d2}', {c.d2}) AS ({metric}, {val})\nFROM {T.name}\n"
               f"ORDER BY id, {metric}")
        code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\n        \"id\",\n        F.stack(F.lit(2), F.lit({q(c.d1)}), F.col(\"{c.d1}\"), "
                f"F.lit({q(c.d2)}), F.col(\"{c.d2}\")).alias(\"{metric}\", \"{val}\"),\n    )\n    .orderBy(\"id\", \"{metric}\")\n)")
        return sql, code, ["id", metric]
    slot = r.choice(["slot", "period", "side"])
    sql = (f"SELECT id, {slot}, qty, score\nFROM (\n    SELECT id, {c.i1}, {c.i2}, {c.d1}, {c.d2}\n    FROM {T.name}\n)\n"
           f"UNPIVOT INCLUDE NULLS (\n    (qty, score) FOR {slot} IN (({c.i1}, {c.d1}) AS first, ({c.i2}, {c.d2}) AS second)\n)\n"
           f"ORDER BY id, {slot}")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .select(\n        \"id\",\n        F.stack(\n            F.lit(2),\n"
            f"            F.lit(\"first\"), F.col(\"{c.i1}\"), F.col(\"{c.d1}\"),\n            F.lit(\"second\"), F.col(\"{c.i2}\"), F.col(\"{c.d2}\"),\n"
            f"        ).alias(\"{slot}\", \"qty\", \"score\"),\n    )\n    .orderBy(\"id\", \"{slot}\")\n)")
    return sql, code, ["id", slot]


def fam_time_bucket(c: Ctx):
    T, r = c.T, c.rng
    if r.random() < 0.6:
        dur = r.choice(["1 hour", "2 hours", "30 minutes", "4 hours"])
        sql = (f"SELECT WINDOW({c.ts}, '{dur}').start AS bucket_start,\n       {c.cat1},\n       COUNT(*) AS n_events,\n"
               f"       SUM({c.i1}) AS total_{c.i1}\nFROM {T.name}\nGROUP BY WINDOW({c.ts}, '{dur}'), {c.cat1}\n"
               f"ORDER BY bucket_start, {c.cat1}")
        code = (IMP + f"\nresult = (\n    df_{T.name}\n    .groupBy(F.window(\"{c.ts}\", \"{dur}\"), \"{c.cat1}\")\n    .agg(\n"
                f"        F.count(F.lit(1)).alias(\"n_events\"),\n        F.sum(\"{c.i1}\").alias(\"total_{c.i1}\"),\n    )\n"
                f"    .select(\n        F.col(\"window.start\").alias(\"bucket_start\"),\n        \"{c.cat1}\",\n        \"n_events\",\n"
                f"        \"total_{c.i1}\",\n    )\n    .orderBy(\"bucket_start\", \"{c.cat1}\")\n)")
        return sql, code, ["bucket_start", c.cat1]
    gap = r.choice(["45 minutes", "90 minutes", "2 hours"])
    sql = (f"SELECT {c.cat1},\n       SESSION_WINDOW({c.ts}, '{gap}').start AS session_start,\n"
           f"       SESSION_WINDOW({c.ts}, '{gap}').end AS session_end,\n       COUNT(*) AS n_events\nFROM {T.name}\n"
           f"GROUP BY {c.cat1}, SESSION_WINDOW({c.ts}, '{gap}')\nORDER BY {c.cat1}, session_start")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .groupBy(\"{c.cat1}\", F.session_window(\"{c.ts}\", \"{gap}\"))\n"
            f"    .agg(F.count(F.lit(1)).alias(\"n_events\"))\n    .select(\n        \"{c.cat1}\",\n"
            f"        F.col(\"session_window.start\").alias(\"session_start\"),\n        F.col(\"session_window.end\").alias(\"session_end\"),\n"
            f"        \"n_events\",\n    )\n    .orderBy(\"{c.cat1}\", \"session_start\")\n)")
    return sql, code, [c.cat1, "session_start"]


def fam_spine(c: Ctx):
    T, r = c.T, c.rng
    day = r.choice(["day", "cal_day", "report_date"])
    tot = f"{c.i1}_total"
    filled = f"{c.i1}_filled"
    sql = (f"WITH bounds AS (\n    SELECT MIN({c.dt}) AS first_day, MAX({c.dt}) AS last_day\n    FROM {T.name}\n),\n"
           f"spine AS (\n    SELECT EXPLODE(SEQUENCE(first_day, last_day, INTERVAL 1 DAY)) AS {day}\n    FROM bounds\n),\n"
           f"daily AS (\n    SELECT {c.dt} AS {day}, SUM({c.i1}) AS {tot}\n    FROM {T.name}\n    WHERE {c.dt} IS NOT NULL\n    GROUP BY {c.dt}\n)\n"
           f"SELECT s.{day},\n       d.{tot},\n       LAST(d.{tot}, TRUE) OVER (ORDER BY s.{day} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {filled}\n"
           f"FROM spine s\nLEFT JOIN daily d ON d.{day} = s.{day}\nORDER BY s.{day}")
    code = (IMPW + f"\nbounds = df_{T.name}.agg(\n    F.min(\"{c.dt}\").alias(\"first_day\"),\n    F.max(\"{c.dt}\").alias(\"last_day\"),\n)\n\n"
            f"spine = bounds.select(\n    F.explode(F.sequence(\"first_day\", \"last_day\", F.expr(\"INTERVAL 1 DAY\"))).alias(\"{day}\")\n)\n\n"
            f"daily = (\n    df_{T.name}\n    .filter(F.col(\"{c.dt}\").isNotNull())\n    .groupBy(F.col(\"{c.dt}\").alias(\"{day}\"))\n"
            f"    .agg(F.sum(\"{c.i1}\").alias(\"{tot}\"))\n)\n\n"
            f"w_fill = Window.orderBy(F.col(\"s.{day}\")).rowsBetween(Window.unboundedPreceding, Window.currentRow)\n"
            f"result = (\n    spine.alias(\"s\")\n    .join(daily.alias(\"d\"), F.col(\"d.{day}\") == F.col(\"s.{day}\"), \"left\")\n"
            f"    .select(\n        F.col(\"s.{day}\").alias(\"{day}\"),\n        F.col(\"d.{tot}\"),\n"
            f"        F.last(F.col(\"d.{tot}\"), ignorenulls=True).over(w_fill).alias(\"{filled}\"),\n    )\n    .orderBy(\"{day}\")\n)")
    return sql, code, [day]


def fam_decimal(c: Ctx):
    T, r = c.T, c.rng
    k = r.choice([3, 5, 10])
    opts = [
        (f"SUM({c.amt}) AS total_{c.amt}", f'F.sum("{c.amt}").alias("total_{c.amt}")'),
        (f"ROUND(AVG({c.amt}), 2) AS avg_{c.amt}", f'F.round(F.avg("{c.amt}"), 2).alias("avg_{c.amt}")'),
        (f"SUM({c.amt} * {c.i2}) AS weighted_{c.amt}", f'F.sum(F.col("{c.amt}") * F.col("{c.i2}")).alias("weighted_{c.amt}")'),
        (f"TRY_DIVIDE(SUM({c.amt}), COUNT({c.i2})) AS {c.amt}_per_{c.i2}",
         f'F.try_divide(F.sum("{c.amt}"), F.count("{c.i2}")).alias("{c.amt}_per_{c.i2}")'),
        (f"CAST(MAX({c.amt}) AS DECIMAL(10, 1)) AS max_{c.amt}_1dp",
         f'F.max("{c.amt}").cast("decimal(10,1)").alias("max_{c.amt}_1dp")'),
        (f"CAST(SUM({c.d1}) AS DECIMAL(14, 2)) AS {c.d1}_dec",
         f'F.sum("{c.d1}").cast("decimal(14,2)").alias("{c.d1}_dec")'),
        (f"MIN({c.amt}) - 1.50 AS min_less_fee", f'(F.min("{c.amt}") - F.lit(1.50).cast("decimal(3,2)")).alias("min_less_fee")'),
        (f"SUM({c.i1}) DIV {k} AS blocks_of_{k}", f'(F.sum("{c.i1}") / {k}).cast("bigint").alias("blocks_of_{k}")'),
    ]
    picks = r.sample(opts, r.randint(3, 5))
    sql = (f"SELECT {c.cat1},\n       " + ",\n       ".join(s for s, _ in picks) +
           f"\nFROM {T.name}\nGROUP BY {c.cat1}\nORDER BY {c.cat1}")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .groupBy(\"{c.cat1}\")\n    .agg(\n        " +
            ",\n        ".join(p for _, p in picks) + f",\n    )\n    .orderBy(\"{c.cat1}\")\n)")
    return sql, code, [c.cat1]


def fam_group_all_tuple(c: Ctx):
    T, r = c.T, c.rng
    if r.random() < 0.5:
        sql = (f"SELECT {c.cat1}, {c.cat2}, COUNT(*) AS n_rows, MAX({c.i1}) AS max_{c.i1}\nFROM {T.name}\n"
               f"WHERE ({c.cat1}, {c.i1}) IN (\n    SELECT {c.cat1}, MAX({c.i1})\n    FROM {T.name}\n    GROUP BY {c.cat1}\n)\n"
               f"GROUP BY ALL\nORDER BY {c.cat1}, {c.cat2}")
        code = (IMP + f"\ntop_{c.i1} = df_{T.name}.groupBy(\"{c.cat1}\").agg(F.max(\"{c.i1}\").alias(\"{c.i1}\"))\n\n"
                f"result = (\n    df_{T.name}\n    .join(top_{c.i1}, [\"{c.cat1}\", \"{c.i1}\"], \"left_semi\")\n"
                f"    .groupBy(\"{c.cat1}\", \"{c.cat2}\")\n    .agg(\n        F.count(F.lit(1)).alias(\"n_rows\"),\n"
                f"        F.max(\"{c.i1}\").alias(\"max_{c.i1}\"),\n    )\n    .orderBy(\"{c.cat1}\", \"{c.cat2}\")\n)")
        return sql, code, [c.cat1, c.cat2]
    sql = (f"SELECT {c.cat1},\n       COUNT(*) AS n_rows,\n       COUNT({c.i2}) AS n_with_{c.i2},\n"
           f"       COUNT(*) - COUNT({c.i2}) AS n_missing_{c.i2},\n       SUM(COALESCE({c.i2}, 0)) AS {c.i2}_or_zero,\n"
           f"       ROUND(AVG(NULLIF({c.d2}, 0)), 2) AS avg_{c.d2},\n       MAX(NVL2({c.cat2}, 1, 0)) AS has_{c.cat2}\n"
           f"FROM {T.name}\nGROUP BY ALL\nORDER BY {c.cat1}")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .groupBy(\"{c.cat1}\")\n    .agg(\n        F.count(F.lit(1)).alias(\"n_rows\"),\n"
            f"        F.count(\"{c.i2}\").alias(\"n_with_{c.i2}\"),\n        (F.count(F.lit(1)) - F.count(\"{c.i2}\")).alias(\"n_missing_{c.i2}\"),\n"
            f"        F.sum(F.coalesce(F.col(\"{c.i2}\"), F.lit(0))).alias(\"{c.i2}_or_zero\"),\n"
            f"        F.round(F.avg(F.nullif(F.col(\"{c.d2}\"), F.lit(0))), 2).alias(\"avg_{c.d2}\"),\n"
            f"        F.max(F.nvl2(F.col(\"{c.cat2}\"), F.lit(1), F.lit(0))).alias(\"has_{c.cat2}\"),\n    )\n    .orderBy(\"{c.cat1}\")\n)")
    return sql, code, [c.cat1]


def fam_struct_collect(c: Ctx):
    T, r = c.T, c.rng
    sf1 = c.sf[0][0]
    lst = f"{singular(c.T.name)}_list"
    sql = (f"SELECT {c.st}.{sf1} AS {sf1},\n       COUNT(*) AS n_rows,\n"
           f"       ARRAY_SORT(COLLECT_LIST(NAMED_STRUCT('id', id, '{c.label}', {c.label}))) AS {lst},\n"
           f"       SORT_ARRAY(COLLECT_SET({c.cat1})) AS {c.cat1}_set\nFROM {T.name}\nWHERE {c.st} IS NOT NULL\n"
           f"GROUP BY {c.st}.{sf1}\nORDER BY {sf1}")
    code = (IMP + f"\nresult = (\n    df_{T.name}\n    .filter(F.col(\"{c.st}\").isNotNull())\n"
            f"    .groupBy(F.col(\"{c.st}.{sf1}\").alias(\"{sf1}\"))\n    .agg(\n        F.count(F.lit(1)).alias(\"n_rows\"),\n"
            f"        F.array_sort(F.collect_list(F.struct(F.col(\"id\").alias(\"id\"), F.col(\"{c.label}\").alias(\"{c.label}\")))).alias(\"{lst}\"),\n"
            f"        F.sort_array(F.collect_set(\"{c.cat1}\")).alias(\"{c.cat1}_set\"),\n    )\n    .orderBy(\"{sf1}\")\n)")
    return sql, code, [sf1]


def fam_pipeline(c: Ctx):
    """3 stages: flatten the array -> per-id aggregate (max_by / count_if) -> semi/anti filter on the child + rank"""
    T, C, r = c.T, c.C, c.rng
    numf = [(n, k) for n, k, _ in c.af if k in ("int", "double")]
    if not numf:
        raise ValueError("no numeric field")
    nf = numf[0][0]
    catf = next((n for n, k, _ in c.af if k == "cat"), c.af[0][0])
    items, per = f"{singular(c.arr)}_items", f"per_{singular(T.name)}"
    th = r.randint(5, 40)
    how_sql = r.choice(["LEFT SEMI", "LEFT ANTI"])
    how_py = "left_semi" if how_sql == "LEFT SEMI" else "left_anti"
    rank_fn = r.choice([("DENSE_RANK", "dense_rank"), ("ROW_NUMBER", "row_number"), ("RANK", "rank")])
    sql = (f"WITH {items} AS (\n    SELECT id, {c.cat1}, e.{nf}, e.{catf}\n    FROM {T.name}\n    LATERAL VIEW EXPLODE({c.arr}) x AS e\n),\n"
           f"{per} AS (\n    SELECT id, {c.cat1},\n           COUNT(*) AS n_items,\n           MAX_BY({catf}, STRUCT({nf}, {catf})) AS top_{catf},\n"
           f"           ROUND(SUM({nf}), 2) AS total_{nf}\n    FROM {items}\n    GROUP BY id, {c.cat1}\n),\n"
           f"kept AS (\n    SELECT p.*\n    FROM {per} p\n    {how_sql} JOIN {C.name} c\n      ON c.{c.fk} = p.id AND c.{c.cnum} >= {th}\n)\n"
           f"SELECT id, {c.cat1}, n_items, top_{catf}, total_{nf},\n"
           f"       {rank_fn[0]}() OVER (PARTITION BY {c.cat1} ORDER BY total_{nf} DESC, id) AS rank_in_{c.cat1}\n"
           f"FROM kept\nORDER BY {c.cat1}, rank_in_{c.cat1}")
    code = (IMPW + f"\n{items} = (\n    df_{T.name}\n    .select(\"id\", \"{c.cat1}\", F.explode(\"{c.arr}\").alias(\"e\"))\n"
            f"    .select(\"id\", \"{c.cat1}\", F.col(\"e.{nf}\"), F.col(\"e.{catf}\"))\n)\n\n"
            f"{per} = (\n    {items}\n    .groupBy(\"id\", \"{c.cat1}\")\n    .agg(\n        F.count(F.lit(1)).alias(\"n_items\"),\n"
            f"        F.max_by(\"{catf}\", F.struct(\"{nf}\", \"{catf}\")).alias(\"top_{catf}\"),\n        F.round(F.sum(\"{nf}\"), 2).alias(\"total_{nf}\"),\n    )\n)\n\n"
            f"kept = (\n    {per}.alias(\"p\")\n    .join(\n        df_{C.name}.alias(\"c\"),\n"
            f"        (F.col(\"c.{c.fk}\") == F.col(\"p.id\")) & (F.col(\"c.{c.cnum}\") >= {th}),\n        \"{how_py}\",\n    )\n    .select(\"p.*\")\n)\n\n"
            f"w_rank = Window.partitionBy(\"{c.cat1}\").orderBy(F.col(\"total_{nf}\").desc(), \"id\")\n"
            f"result = (\n    kept\n    .select(\n        \"id\",\n        \"{c.cat1}\",\n        \"n_items\",\n        \"top_{catf}\",\n"
            f"        \"total_{nf}\",\n        F.{rank_fn[1]}().over(w_rank).alias(\"rank_in_{c.cat1}\"),\n    )\n"
            f"    .orderBy(\"{c.cat1}\", \"rank_in_{c.cat1}\")\n)")
    return sql, code, [c.cat1, f"rank_in_{c.cat1}"]


FAMILIES = {  # name: (function, weight)
    "posexplode": (fam_posexplode, 8), "inline": (fam_inline, 7), "map": (fam_map, 9), "json": (fam_json, 8),
    "hof": (fam_hof, 9), "array_fns": (fam_array_fns, 7), "by_agg": (fam_by_agg, 7), "semi_anti": (fam_semi_anti, 9),
    "setop": (fam_setop, 5), "range_frame": (fam_range_frame, 7), "unpivot": (fam_unpivot, 6),
    "time_bucket": (fam_time_bucket, 6), "spine": (fam_spine, 4), "decimal": (fam_decimal, 6),
    "group_all_tuple": (fam_group_all_tuple, 5), "struct_collect": (fam_struct_collect, 4), "pipeline": (fam_pipeline, 10),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=6000)
    ap.add_argument("--seed", type=int, default=20)
    ap.add_argument("--out", default=str(HERE / "raw" / "cands.jsonl"))
    a = ap.parse_args()
    rng = random.Random(a.seed)
    names = list(FAMILIES)
    weights = [FAMILIES[n][1] for n in names]
    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n_ok, errs = 0, {}
    with open(out, "w") as f:
        i = 0
        while n_ok < a.n:
            i += 1
            fam = rng.choices(names, weights)[0]
            dom = rng.choice(TRAIN_DOMAINS)
            try:
                c = Ctx(random.Random(f"{a.seed}-{i}"), dom)
                sql, code, order = FAMILIES[fam][0](c)
            except Exception as e:  # noqa: BLE001
                errs[fam] = errs.get(fam, 0) + 1
                if errs[fam] <= 2:
                    print(f"gen error {fam}: {type(e).__name__}: {e}")
                continue
            if not c.up:
                sql = low_kw(sql)
            f.write(json.dumps({"id": f"nv{i:06d}", "family": fam, "domain": dom, "sql": sql, "code": code,
                                "order_cols": order, "tables": {c.T.name: c.T.sql(), c.C.name: c.C.sql()}}) + "\n")
            n_ok += 1
    print(f"{n_ok} candidates -> {out}; generator errors {errs}")


if __name__ == "__main__":
    main()
