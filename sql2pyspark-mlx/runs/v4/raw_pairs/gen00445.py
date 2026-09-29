from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service").isin(["search", "auth", "billing"]))
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.col("context")["version"].alias("version"),
    )
)
