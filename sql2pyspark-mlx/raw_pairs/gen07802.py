from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("service") == "search").alias("search_events"),
    )
)
