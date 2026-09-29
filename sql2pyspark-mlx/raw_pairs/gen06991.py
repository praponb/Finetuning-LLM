from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") == "billing")
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.col("context").getItem("trace").alias("trace"),
    )
)

result = (
    flattened_attrs
    .withColumn("retry_count_tier", F.when(F.col("retry_count") >= 4, "LARGE").otherwise("SMALL"))
    .cube("retry_count_tier")
    .agg(
        F.avg("status_code_high").alias("avg_status_code_high"),
        F.round(F.avg("retry_count"), 2).alias("avg_retry_count"),
        F.count(F.when(F.col("service") == "auth", 1)).alias("auth_rows"),
        F.count("*").alias("row_count"),
        F.min("log_ts").alias("first_log_ts"),
        F.grouping("retry_count_tier").alias("grouping_level"),
    )
    .orderBy("avg_status_code_high")
)
