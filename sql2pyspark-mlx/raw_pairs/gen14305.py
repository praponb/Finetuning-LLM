from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "attr_value"),
    )
)

result = flattened.select("id", "service", "log_ts", "level", "latency_ms", "k")
