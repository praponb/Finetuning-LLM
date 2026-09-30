from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "v"),
    )
)

result = (
    exploded
    .select("id", "log_ts", "level", "latency_ms", "v")
    .orderBy(F.col("level").desc_nulls_last())
)
