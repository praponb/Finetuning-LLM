from pyspark.sql import functions as F

parsed = (
    df_app_logs
    .filter(F.col("level").isin(["INFO", "DEBUG"]))
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("latency_ms") >= 2289, "PREMIUM").otherwise("STANDARD").alias("latency_ms_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy("event_count")
)
