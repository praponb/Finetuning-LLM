from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("status_code") <= 435)
    .select(
        "id",
        "status_code",
        "payload_json",
        "context",
        F.col("context")["trace"].alias("trace"),
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("has_fault"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    unpacked
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 463, "LARGE").otherwise("SMALL"))
    .rollup("status_code_tier", "tier")
    .agg(
        F.sort_array(F.collect_set("tier")).alias("tier_list"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.grouping("status_code_tier").alias("grouping_id"),
    )
    .orderBy("status_code_tier")
)
