from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    unpacked
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.when(F.col("latency_ms") >= 1821, "HIGH").otherwise("LOW").alias("latency_ms_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("latency_ms", 0.9).alias("latency_ms_p90"),
        F.max("log_ts").alias("last_seen"),
        F.min("log_ts").alias("started_at"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms_2"),
    )
    .orderBy(F.col("avg_latency_ms").asc_nulls_first())
)
