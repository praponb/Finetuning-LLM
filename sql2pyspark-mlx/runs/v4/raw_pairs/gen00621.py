from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("status_code") < 287)
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        "status_code",
        "payload_json",
        "context",
        F.col("context")["region"].alias("region"),
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("flag_bit1") >= 1, "HIGH").otherwise("LOW").alias("flag_bit1_tier"),
        "host",
    )
    .agg(
        F.sum(F.when(F.col("host") == "h1", F.col("latency_ms")).otherwise(0)).alias("h1_latency_ms"),
        F.count_if(F.col("host") == "h1").alias("h1_events"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.countDistinct("service").alias("distinct_services"),
        F.size(F.collect_set("host")).alias("num_hosts"),
    )
    .filter(F.col("avg_latency_ms") < 0)
)
