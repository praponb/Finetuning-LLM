from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.corr("latency_ms", "status_code").alias("corr_latency_ms_status_code"),
        F.count("*").alias("event_count"),
    )
    .orderBy("level")
)
