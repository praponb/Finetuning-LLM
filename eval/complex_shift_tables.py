"""Tables for `eval/evaluate.py --tables` on the complex schema-shift set
(sql2pyspark-mlx/complex_v16/data/complex_shift_meta.jsonl): the 5 schema_v15 HELDOUT_DOMAINS rendered as complex_v8
tables (complex_v8 sample rows: clustered timestamps), which v16 training never sees.

evaluate.py loads --tables files with importlib without registering them in sys.modules, so this wrapper imports the
patch module normally.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sql2pyspark-mlx" / "complex_v16"))

import cx_patch  # noqa: E402


def make_dataframes(spark):
    import catalog as cx
    tables = cx_patch.synth_tables(True)
    frames = {}
    for t in tables:
        df = spark.createDataFrame(cx.make_rows(t), t.ddl)
        df.createOrReplaceTempView(t.name)
        frames[f"df_{t.name}"] = df
    return frames
