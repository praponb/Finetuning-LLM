"""Sample tables for eval/harness_suite.jsonl: the 8 source tables of sql2pyspark_harness/sql2pyspark_test_suite.sql.

The suite builds its data as TEMP VIEWs from inline VALUES (customers, products, orders, order_items, payments, fx_rates,
web_events, promotions), with maps, arrays, structs, JSON strings, DECIMAL money and timestamps. Here the setup
statements are run once and each view is materialized into a plain DataFrame (so `df_<table>` does not depend on the
view it came from), re-registered under the same name, and returned as {"df_<table>": DataFrame}. Five names
(customers, products, orders, order_items, payments) replace schemas.py tables of the same name while loaded.

Used by:  python eval/evaluate.py --set eval/harness_suite.jsonl --tables eval/harness_tables.py ...
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HARNESS = ROOT / "sql2pyspark_harness"
SUITE = HARNESS / "sql2pyspark_test_suite.sql"


def load_suite(suite: Path = SUITE):
    """The harness's own parser (compare_harness.load_suite): [Statement(id, sql)] with full-line comments removed."""
    spec = importlib.util.spec_from_file_location("compare_harness", HARNESS / "compare_harness.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["compare_harness"] = mod          # its @dataclass needs the module registered
    spec.loader.exec_module(mod)
    return mod.load_suite(suite)


def make_dataframes(spark, suite: Path = SUITE) -> dict:
    """Run the setup statements, materialize each view, return {"df_<table>": DataFrame} (views re-registered)."""
    frames = {}
    for s in load_suite(suite):
        if not s.is_setup:
            continue
        spark.sql(s.sql)
        name = s.id.split(":", 1)[1]
        view = spark.table(name)
        df = spark.createDataFrame(view.collect(), view.schema).cache()
        df.count()
        df.createOrReplaceTempView(name)
        frames[f"df_{name}"] = df
    return frames
