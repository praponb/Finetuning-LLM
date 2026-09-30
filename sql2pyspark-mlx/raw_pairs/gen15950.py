from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .rollup(
        F.date_trunc("DAY", "log_ts").alias("activity_day"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
    )
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
    .orderBy(F.col("activity_day").desc())
)
