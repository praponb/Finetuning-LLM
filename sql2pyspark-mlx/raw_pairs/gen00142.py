from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select("id", "service", "host", "log_ts", "status_code", "attr_key", "v")
    .orderBy(F.col("host").desc_nulls_last())
)
