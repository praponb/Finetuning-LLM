from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key", "host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("YEAR", "log_ts"), "host").orderBy("status_code", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("attr_key", "host").orderBy("latency_ms", "id", "attr_key").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("latency_ms").asc_nulls_first())
w5 = Window.partitionBy("attr_key", "host").orderBy(F.col("status_code").desc(), "id", "attr_key")
w6 = Window.partitionBy(F.year("log_ts")).orderBy("cumulative_status_code", "id", "attr_key")
w7 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w8 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

with_windows = (
    lines
    .select(
        "*",
        F.max("status_code").over(w).alias("status_code_max_15m"),
        F.sum("status_code").over(w2).alias("cumulative_status_code"),
        F.avg("latency_ms").over(w3).alias("latency_ms_avg_5"),
        F.cume_dist().over(w4).alias("latency_ms_cume_dist"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.ntile(5).over(w5).alias("status_code_bucket"),
        F.ntile(3).over(w6).alias("cumulative_status_code_bucket"),
    )
)

valid = (
    enriched
    .filter((~F.col("attr_key").eqNullSafe(F.lit("version"))) & (F.col("latency_ms_avg_5") > 736))
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "attr_value",
        "status_code_max_15m",
        "cumulative_status_code",
        "latency_ms_cume_dist",
        "status_code_bucket",
        "cumulative_status_code_bucket",
        F.col("latency_ms_avg_5").cast("string").alias("latency_ms_avg_5_str"),
        F.round((F.col("status_code_max_15m") / F.col("status_code_bucket")), 2).alias("status_code_max_15m_per_status_code_bucket"),
    )
)

filtered = (
    valid
    .filter(F.col("attr_key").isNotNull() & (F.col("cumulative_status_code_bucket") < 8))
    .select(
        "id",
        "log_ts",
        "status_code",
        "attr_key",
        "status_code_max_15m",
        "latency_ms_cume_dist",
        "cumulative_status_code_bucket",
        "latency_ms_avg_5_str",
        "status_code_max_15m_per_status_code_bucket",
        F.lower("attr_key").alias("attr_key_lower"),
        F.coalesce("attr_key", F.lit("NONE")).alias("resolved_attr_key"),
    )
)

prev_events = filtered.withColumn("prev_log_ts", F.lag("log_ts").over(w7))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_app_logs = (
    with_gaps
    .select(
        "id",
        "log_ts",
        "attr_key",
        "status_code_max_15m_per_status_code_bucket",
        "resolved_attr_key",
        F.sum("new_group_flag").over(w8).alias("session_id"),
    )
)

result = (
    sessions_app_logs
    .groupBy("session_id")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.round(F.sum("session_id") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("session_id_per_version"),
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
    )
    .orderBy(F.col("session_id").asc_nulls_first())
)
