from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "level",
        "status_code",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)

result = (
    unpacked
    .groupBy("level")
    .agg(
        F.size(F.collect_set("tier")).alias("num_tiers"),
        F.sum(F.when(F.col("tier") == "free", F.col("status_code")).otherwise(0)).alias("free_status_code"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") > 1)
)
