from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = exploded.select("id", "host", "level", "latency_ms", "attr_key", "v")
