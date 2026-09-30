from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    flattened
    .select("id", "level", "latency_ms", "k", "attr_value")
    .orderBy(F.col("k").desc())
)
