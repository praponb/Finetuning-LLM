from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = lines.select("id", "service", "host", "log_ts", "status_code", "attr_key", "v")
