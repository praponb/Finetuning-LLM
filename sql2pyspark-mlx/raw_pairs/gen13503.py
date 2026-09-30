from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "status_code",
        "payload_json",
        "context",
        F.size("context").alias("num_context"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("status_code") >= 403, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
    )
    .agg(
        F.avg("status_code").alias("avg_status_code"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
    )
    .orderBy(F.col("cnt").desc())
)
