from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("log_ts") < "2025-09-27")
    .select(
        "id",
        "log_ts",
        "payload_json",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.col("context")["region"].alias("region"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context").getItem("trace").alias("trace"),
        F.col("context")["version"].alias("version"),
    )
)

result = (
    unpacked
    .rollup("tier", F.date_trunc("day", "log_ts").alias("activity_day"))
    .agg(
        F.countDistinct("trace").alias("distinct_traces"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("span_minutes"),
        F.size(F.collect_set("tier")).alias("num_tiers"),
    )
    .filter(F.col("num_tiers") >= 1)
)
