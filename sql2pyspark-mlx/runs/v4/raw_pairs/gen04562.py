from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    flattened
    .select("id", "service", "host", "level", "key", "v")
    .orderBy(F.col("key").asc_nulls_first())
)
