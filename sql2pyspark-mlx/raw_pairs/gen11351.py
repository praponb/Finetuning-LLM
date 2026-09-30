from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("k", "v"),
    )
)

result = (
    lines
    .select("id", "service", "host", "level", "latency_ms", "k", "v")
    .orderBy("k")
)
