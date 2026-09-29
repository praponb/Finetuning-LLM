from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "host")
    .orderBy(F.col("host").desc())
)
