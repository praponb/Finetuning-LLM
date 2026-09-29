from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .select("id", "service", "log_ts", "latency_ms", "attr_value")
    .orderBy(F.col("service").desc_nulls_last())
)
