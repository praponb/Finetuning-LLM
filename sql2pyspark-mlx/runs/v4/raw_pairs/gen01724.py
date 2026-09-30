from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "log_ts").alias("hour_window"))
    .agg(
        F.avg(F.when(F.col("level") == "INFO", F.col("status_code"))).alias("info_avg_status_code"),
        F.min("log_ts").alias("started_at"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("service") == "checkout", 1)), F.lit(0)), 2).alias("status_code_per_checkout"),
    )
)
