from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "log_ts",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.coalesce(F.col("context")["version"], F.lit("unknown")).alias("version_or_default"),
    )
)
