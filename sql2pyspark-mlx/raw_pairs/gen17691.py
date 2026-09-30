from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.col("context").getItem("trace").alias("trace"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    parsed
    .cube(
        F.when(F.col("retry_count") >= 4, "LARGE").otherwise("SMALL").alias("retry_count_tier"),
    )
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("retry_count")).otherwise(0)).alias("checkout_retry_count"),
        F.sum(F.when(F.col("host") == "h3", F.col("retry_count")).otherwise(0)).alias("h3_retry_count"),
        F.round(F.avg("retry_count"), 2).alias("avg_retry_count"),
        F.max(F.when(F.col("service") == "billing", F.col("retry_count"))).alias("billing_max_retry_count"),
    )
)
