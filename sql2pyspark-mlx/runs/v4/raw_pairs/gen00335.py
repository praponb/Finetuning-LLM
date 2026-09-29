from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    flattened
    .groupBy("level")
    .agg(
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
    .orderBy(F.col("level").desc_nulls_first())
)
