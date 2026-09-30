from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)
