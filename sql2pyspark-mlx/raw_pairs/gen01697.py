from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .filter(F.col("service") == "search")
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .groupingSets([["service", "level"], ["service"], []], "service", "level")
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
        F.count_if(F.col("service") == "billing").alias("billing_events"),
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("num_levels").desc())
)
