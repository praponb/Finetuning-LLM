"""Sample tables for eval/harness_suite_v2.jsonl: the 13 source tables of sql2pyspark_sql_suite/suite_all.sql.

suite_all.sql = Part 1 (byte-identical to sql2pyspark_harness/sql2pyspark_test_suite.sql: 8 tables, Q01-Q12) +
Part 2 (5 more tables: inventory_movements, tier_history, category_tree, support_tickets, price_history; Q13-Q26).
Same loader as eval/harness_tables.py, only the suite file differs.

Used by:  python eval/evaluate.py --set eval/harness_suite_v2.jsonl --tables eval/harness_tables_v2.py ...
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import harness_tables as ht  # noqa: E402

SUITE = ht.ROOT / "sql2pyspark_sql_suite" / "suite_all.sql"


def load_suite():
    return ht.load_suite(SUITE)


def make_dataframes(spark) -> dict:
    return ht.make_dataframes(spark, SUITE)
