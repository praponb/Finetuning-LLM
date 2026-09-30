from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
        F.size("context").alias("num_context"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.coalesce(F.col("context").getItem("region"), F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "log_ts").alias("activity_day"), "host")
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("host") == "h3", F.col("num_context")).otherwise(0)).alias("h3_num_context"),
        F.sum(F.when(F.col("level") == "INFO", F.col("retry_count")).otherwise(0)).alias("info_retry_count"),
        F.avg("status_code_high").alias("avg_status_code_high"),
    )
    .filter(F.col("row_count") > 1)
)
