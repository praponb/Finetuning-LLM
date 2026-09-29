from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("day", "log_ts").alias("activity_day"), "host")
    .agg(
        F.size(F.collect_set("level")).alias("num_levels"),
        F.min("log_ts").alias("first_seen"),
    )
    .orderBy(F.col("activity_day").desc())
)
