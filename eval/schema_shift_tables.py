"""Tables for `eval/evaluate.py --tables` on the schema-shift set (sql2pyspark-mlx/schema_v15/data/schema_shift_meta.jsonl):
the 5 synthetic domains that v15 training never sees (synth_catalog.HELDOUT_DOMAINS), incl. their nested tables.

evaluate.py loads --tables files with importlib without registering them in sys.modules, so this wrapper (no
dataclasses) imports the catalog normally.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sql2pyspark-mlx" / "schema_v15"))

import synth_catalog as _sc  # noqa: E402


def make_dataframes(spark):
    doms = [v for d, v in _sc.CATALOG.items() if d in _sc.HELDOUT_DOMAINS]
    return _sc.make_dataframes(spark, tables=[t for v in doms for t in v["tables"] + [v["nested"]]])
