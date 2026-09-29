from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        F.col("context").getItem("trace").alias("trace"),
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
    )
)

result = (
    parsed
    .withColumn("activity_day", F.date_trunc("DAY", "log_ts"))
    .groupingSets(
        [["host", "activity_day", "service"], ["host", "activity_day"], ["host"], []],
        "host",
        "activity_day",
        "service",
    )
    .agg(
        F.countDistinct("region_or_default").alias("distinct_region_or_defaults"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.max("log_ts").alias("last_seen"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_region_or_defaults") > 1)
)
