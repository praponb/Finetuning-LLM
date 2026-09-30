from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    lines
    .select("id", "log_ts", "level", "v")
    .orderBy("level", "id")
)
