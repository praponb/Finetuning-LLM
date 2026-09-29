from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .select("id", "status_code", "v")
    .orderBy(F.col("status_code").desc_nulls_last(), F.col("id").desc())
)
