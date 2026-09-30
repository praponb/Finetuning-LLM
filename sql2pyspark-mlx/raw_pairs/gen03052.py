from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") != "billing")
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.sum(F.when(F.col("service") == "checkout", F.col("retry_count"))).alias("checkout_total_retry_count"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
    )
    .filter(F.col("avg_latency_ms") > 0)
    .orderBy("hour_window")
)
