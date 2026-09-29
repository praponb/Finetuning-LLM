from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["region"].alias("region"),
        F.col("context").getItem("trace").alias("trace"),
    )
)

result = (
    parsed
    .groupBy("tier")
    .agg(
        F.size(F.collect_set("tier")).alias("num_tiers"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.countDistinct("level").alias("distinct_levels"),
        F.countDistinct("user_tier").alias("distinct_user_tiers"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .orderBy(F.col("distinct_user_tiers").asc_nulls_first())
)
