from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "attr_key").orderBy("latency_ms_whole", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("latency_ms", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("latency_ms_whole_minus_latency_ms", "id", "attr_key")
w4 = Window.partitionBy("attr_key")
w_hist = Window.orderBy(F.col("row_seq_per_seq_no").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

base = (
    lines
    .filter(F.col("log_ts") > F.lit("2025-06-01 08:00:00").cast("timestamp"))
    .select(
        "id",
        "host",
        "latency_ms",
        "attr_key",
        F.floor("latency_ms").alias("latency_ms_whole"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

cleaned = (
    ranked
    .filter(F.col("attr_key").isNotNull() & (F.col("latency_ms") != 2100))
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "latency_ms_whole",
        "dedup_rank",
        F.concat("host", F.lit(" "), "attr_key").alias("host_attr_key"),
        (F.col("latency_ms_whole") - F.col("latency_ms")).alias("latency_ms_whole_minus_latency_ms"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_clean"),
    )
)

metrics = (
    cleaned
    .filter(F.col("latency_ms").isNotNull())
    .select(
        "*",
        F.lead("latency_ms_whole").over(w2).alias("next_latency_ms_whole"),
        F.lag("latency_ms_whole").over(w2).alias("prev_latency_ms_whole"),
        F.lead("latency_ms").over(w2).alias("next_latency_ms"),
        F.lag("latency_ms").over(w2).alias("prev_latency_ms"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("attr_key").isNotNull() & (~F.col("attr_key").eqNullSafe(F.lit("trace"))))
    .select(
        "id",
        "latency_ms",
        "attr_key",
        "latency_ms_whole",
        "host_attr_key",
        "latency_ms_whole_minus_latency_ms",
        "next_latency_ms_whole",
        "next_latency_ms",
        "row_seq",
        F.round((F.col("row_seq") / F.col("seq_no")), 2).alias("row_seq_per_seq_no"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.count("row_seq_per_seq_no").over(w4).alias("attr_key_count_row_seq_per_seq_no"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)

result = (
    with_windows
    .select("id", "attr_key", "row_seq_per_seq_no", "latency_ms_whole_minus_latency_ms")
    .unpivot(
        ids=["id", "attr_key"],
        values=["row_seq_per_seq_no", "latency_ms_whole_minus_latency_ms"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
