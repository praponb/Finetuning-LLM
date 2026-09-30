from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "latency_ms",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("direct")).alias("trace_or_default"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
    )
)
