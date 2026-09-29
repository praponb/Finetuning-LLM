from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "level", "k", "attr_value")
    .orderBy(F.col("level").desc(), "id")
)
