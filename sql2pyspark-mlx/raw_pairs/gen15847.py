from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("latency_ms").between(863, 1490))
    .select(
        "id",
        "latency_ms",
        "status_code",
        "context",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("latency_ms") >= 2586, "LARGE").otherwise("SMALL").alias("latency_ms_tier"),
    )
    .agg(
        F.min("status_code_high").alias("min_status_code_high"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("status_code", 0.5).alias("status_code_p50"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .orderBy(F.col("min_status_code_high").desc_nulls_first())
)
