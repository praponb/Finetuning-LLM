from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "level",
        "latency_ms",
        "payload_json",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
    )
)

result = (
    unpacked
    .groupBy("service")
    .agg(
        F.round(F.sum("latency_ms") / F.nullif(F.count(F.when(F.col("level") == "ERROR", 1)), F.lit(0)), 2).alias("latency_ms_per_error"),
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.sum(F.when(F.col("level") == "WARN", F.col("latency_ms"))).alias("warn_total_latency_ms"),
    )
    .orderBy(F.col("search_events").asc_nulls_first())
)
