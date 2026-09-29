from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        "service",
        F.when(F.col("retry_count") >= 4, "HIGH").otherwise("LOW").alias("retry_count_tier"),
    )
    .agg(
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.percentile_approx("latency_ms", [0.25, 0.5, 0.75]).alias("latency_ms_quartiles"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
    )
)
