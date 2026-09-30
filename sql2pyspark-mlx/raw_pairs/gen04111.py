from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("key").isNotNull() & (F.col("latency_ms") == 1778))
    .withColumn("service_length", F.length("service"))
    .withColumn("clean_level", F.trim("level"))
    .withColumn("resolved_key", F.coalesce("key", F.lit("NONE")))
)

filtered = (
    cleaned
    .filter((F.col("log_ts") > F.lit("2025-06-01").cast("date")) & F.col("key").isin(["version", "trace"]))
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "service_length",
        "resolved_key",
        F.dayofmonth("log_ts").alias("log_ts_day"),
    )
)

with_windows = (
    filtered
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        "resolved_key",
        "log_ts_day",
        F.sum("log_ts_day").over(w_running).alias("log_ts_day_sum_15m"),
        F.avg("log_ts_day").over(w_running).alias("log_ts_day_avg_15m"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.percentile_approx("log_ts_day", 0.95).alias("log_ts_day_p95"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("log_ts_day", [0.25, 0.5, 0.75]).alias("log_ts_day_quartiles"),
        F.percentile_approx("log_ts_day", 0.9).alias("log_ts_day_p90"),
        F.min("log_ts").alias("started_at"),
    )
    .filter(F.col("log_ts_day_p95") > 0)
    .orderBy(F.col("log_ts_day_p95").desc_nulls_first())
)
