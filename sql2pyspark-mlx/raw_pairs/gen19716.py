from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("service").isin(["search", "billing"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["version"].alias("version"),
    )
)
