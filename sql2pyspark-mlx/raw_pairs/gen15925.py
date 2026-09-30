from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    exploded
    .groupBy(
        "service",
        F.when(F.col("status_code") >= 466, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count_if(F.col("service") == "search").alias("search_events"),
    )
    .filter(F.col("avg_latency_ms") < 1000000)
    .orderBy(F.col("corr_latency_ms_status_code").asc_nulls_first())
)
