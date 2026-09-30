from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.max("latency_ms").alias("max_latency_ms"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
