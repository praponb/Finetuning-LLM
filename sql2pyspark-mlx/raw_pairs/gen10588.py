from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("host").isin(["h3", "h2"]))
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
    )
)

result = (
    parsed
    .groupBy("host")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("log_ts").alias("last_log_ts"),
    )
    .orderBy("distinct_ids")
)
