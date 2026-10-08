"""Point the complex_v8 generator (../complex_v8/generate.py) at the synthetic schema_v15 catalog.

generate.py does `from catalog import TABLES, TABLE_BY_NAME, SINGULAR, ...` when it is imported, and verify.py's
checker builds its DataFrames from `catalog.TABLES`, so install() must run before either is imported (also in every
spawned verify worker: init_verify_worker). The flat synthetic tables are converted to complex_v8 `Col` / `Table`
(their domain kept); sample rows then come from complex_v8's `make_rows` (clustered timestamps, 40 rows).
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
# complex_v8 first: schema_v15 also has a generate.py / verify.py
for p in (str(MLX), str(MLX / "schema_v15"), str(MLX / "complex_v8")):
    if p in sys.path:
        sys.path.remove(p)
    sys.path.insert(0, p)


def synth_tables(heldout: bool | None):
    """complex_v8 Tables for the synthetic domains: training (False), held-out (True) or all (None)"""
    import catalog as cx
    import synth_catalog as sc
    out = []
    for dom, v in sc.CATALOG.items():
        if heldout is not None and (dom in sc.HELDOUT_DOMAINS) != heldout:
            continue
        for t in v["tables"]:
            cols = tuple(cx.Col(c.name, c.kind, pool=c.pool, lo=c.lo, hi=c.hi,
                                ref=(t.name if c.kind == "id" else c.ref), nullable=c.nullable) for c in t.columns)
            out.append(cx.Table(t.name, dom, cols, 40))
    return out


def install(heldout: bool | None) -> None:
    if "generate" in sys.modules:
        raise RuntimeError("install() must run before importing complex_v8/generate.py")
    import catalog as cx
    import synth_catalog as sc
    tables = synth_tables(heldout)
    cx.TABLES = tuple(tables)
    cx.TABLE_BY_NAME.clear()
    cx.TABLE_BY_NAME.update({t.name: t for t in tables})
    cx.DOMAINS[:] = sorted({t.domain for t in tables})
    cx.SINGULAR.clear()
    cx.SINGULAR.update({t.name: sc.singular(t.name) for t in tables})
    cx._ROWS.clear()


def init_verify_worker() -> None:
    install(None)
    import verify as cv      # complex_v8/verify.py
    cv._init_worker()


def rebind(gen) -> None:
    """after `import generate`: replace the generator's hard-coded complex_v8 table lists with synthetic ones"""
    import inspect
    from catalog import TABLES
    gen.ALL_TABLES = [t.name for t in TABLES]
    gen.FACT_TABLES = [t.name for t in TABLES if any(c.kind == "fk" for c in t.columns)]
    splits = []
    for t in TABLES:
        cat = [c for c in t.columns if c.kind == "cat" and not c.nullable and len(c.pool) >= 3]
        keys = [c.name for c in t.columns if c.kind == "fk"]
        if cat and keys and any(c.kind in ("ts", "date") for c in t.columns):
            nums = [c.name for c in t.columns if c.kind in ("int", "double")]
            splits.append((t.name, cat[0].name, list(cat[0].pool), nums[0] if nums else None, keys))
    gen.SYNTH_SPLITS = splits
    src = inspect.getsource(gen.fin_full_outer)
    a, b = src.index("    splits = ["), src.index("    tbl, cat, vals, m, keys")
    exec(src[:a] + "    splits = SYNTH_SPLITS\n" + src[b:], gen.__dict__)
