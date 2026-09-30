from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)

result = (
    parsed
    .groupBy("host", F.date_trunc("DAY", "log_ts").alias("activity_day"))
    .agg(
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
