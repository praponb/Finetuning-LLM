from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .select("id", "service", "host", "log_ts", "status_code", "key", "value")
    .orderBy(F.col("host").desc())
)
