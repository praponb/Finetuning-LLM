from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    lines
    .select("id", "log_ts", "level", "latency_ms", "k", "value")
    .orderBy("log_ts", "k")
)
