from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = exploded.select("service", "host", "latency_ms", "key", "attr_value")
