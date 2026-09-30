from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("log_ts"), "service").orderBy("latency_ms", "id", "attr_key").rowsBetween(-5, -1)
w2 = Window.partitionBy("host", "service")
w3 = Window.orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w5 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("attr_key").orderBy(F.col("log_ts").desc(), "id", "attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

scored = (
    lines
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "attr_value",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_5_prior"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w2)), 4).alias("latency_ms_share"),
        F.first("latency_ms", ignorenulls=True).over(w3).alias("first_known_latency_ms"),
    )
)

flagged = (
    scored
    .withColumn("is_new_burst", F.when(F.lag("log_ts").over(w4).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w4) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "latency_ms_avg_5_prior",
        F.sum("is_new_burst").over(w5).alias("session_no"),
    )
)

ranked = (
    islands
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w4).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w4) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w5))

base = (
    sessions_app_logs
    .filter((F.col("log_ts") >= F.lit("2025-06-01 14:00:00").cast("timestamp")) & F.col("attr_key").isNotNull())
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "latency_ms_avg_5_prior",
        "session_no",
        "rn",
        "session_start_flag",
        F.year("log_ts").alias("log_ts_year"),
    )
)

with_prev = base.withColumn("prev_log_ts", F.lag("log_ts").over(w4))

gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands_app_logs = gaps.withColumn("session_id", F.sum("new_group_flag").over(w5))

result = (
    islands_app_logs
    .groupBy("attr_key")
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.round(F.sum("session_no") / F.nullif(F.count(F.when(F.col("attr_key") == "version", 1)), F.lit(0)), 2).alias("session_no_per_version"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("latency_ms_avg_5_prior")).otherwise(0)).alias("region_latency_ms_avg_5_prior"),
    )
)
