from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select("level", "status_code", "k", "value")
    .orderBy(F.col("level").desc())
)
