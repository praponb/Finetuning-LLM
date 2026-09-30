from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .select("id", "service", "log_ts", "status_code", "key", "v")
    .orderBy(F.col("id").desc())
)
