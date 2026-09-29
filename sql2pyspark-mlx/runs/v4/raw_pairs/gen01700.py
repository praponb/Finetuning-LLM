from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "host", "latency_ms", F.explode_outer("context").alias("k", "value"))
)

result = (
    flattened
    .select("id", "host", "k")
    .orderBy(F.col("k").desc_nulls_last())
)
