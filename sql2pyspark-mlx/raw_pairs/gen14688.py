from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)
