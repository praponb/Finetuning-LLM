from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service").isin(["search", "checkout", "auth"]))
    .select(
        "id",
        "service",
        "latency_ms",
        "status_code",
        "payload_json",
        F.col("context").getItem("trace").alias("trace"),
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
    )
)
