from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("service")
    .agg(
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("service")).alias("num_services"),
        F.max("log_ts").alias("last_seen"),
    )
    .orderBy(F.col("service").desc_nulls_first())
)
