from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("key") == "version", 1)), F.lit(0)), 2).alias("latency_ms_per_version"),
    )
    .orderBy(F.col("distinct_hosts").desc_nulls_last())
)
