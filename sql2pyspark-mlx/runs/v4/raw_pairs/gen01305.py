from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("log_ts") < "2025-02-18")
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
    )
)
