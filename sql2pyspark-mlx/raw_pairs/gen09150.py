from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .select("id", "log_ts", "latency_ms", "status_code", "attr_key")
    .orderBy(F.col("log_ts").desc_nulls_first())
)
