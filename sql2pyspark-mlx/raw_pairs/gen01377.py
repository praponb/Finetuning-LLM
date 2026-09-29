from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        "status_code",
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.size("context").alias("num_context"),
        F.coalesce(F.col("context").getItem("trace"), F.lit("unknown")).alias("trace_or_default"),
    )
)
