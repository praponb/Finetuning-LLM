from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    lines
    .select("id", "k")
    .orderBy("id")
)
