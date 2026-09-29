from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("status_code") >= 458, "LARGE").otherwise("SMALL").alias("status_code_tier"),
    )
    .agg(
        F.percentile_approx("status_code", 0.95).alias("status_code_p95"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90_2"),
    )
    .filter(F.col("status_code_p95") > 0)
    .orderBy(F.col("status_code_p90").desc_nulls_last())
)
