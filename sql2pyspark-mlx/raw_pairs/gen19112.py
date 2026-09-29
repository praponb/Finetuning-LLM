from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("latency_ms") <= 1581)
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("unknown")).alias("trace_or_default"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
        F.sum(F.when(F.col("level") == "ERROR", F.col("latency_ms"))).alias("error_total_latency_ms"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
    )
)
