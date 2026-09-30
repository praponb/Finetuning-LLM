from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("attr_key").orderBy("status_code_high", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

parsed = (
    lines
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        F.shiftright("status_code", 2).alias("status_code_high"),
        (F.col("status_code").bitwiseAND(8) != 0).cast("int").alias("flag_bit8"),
    )
)

valid = (
    parsed
    .filter(F.col("log_ts") > F.lit("2025-06-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "status_code_high",
        F.initcap(F.trim("attr_key")).alias("attr_key_title"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "latency_ms",
        "attr_key",
        F.lag("latency_ms").over(w_recent).alias("prev_latency_ms"),
        F.lead("latency_ms").over(w_recent).alias("next_latency_ms"),
        F.lead("status_code_high").over(w_recent).alias("next_status_code_high"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("latency_ms").over(w2).alias("latency_ms_sum_30m"),
    )
)

result = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.sum(F.when(F.col("attr_key") == "trace", F.col("latency_ms_sum_30m")).otherwise(0)).alias("trace_latency_ms_sum_30m"),
        F.round(F.sum("latency_ms_sum_30m") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("latency_ms_sum_30m_per_version"),
    )
    .orderBy(F.col("trace_latency_ms_sum_30m").desc())
)
