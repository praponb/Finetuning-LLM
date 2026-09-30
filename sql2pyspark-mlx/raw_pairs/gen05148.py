from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "payload_json",
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.coalesce(F.col("context").getItem("version"), F.lit("unknown")).alias("version_or_default"),
        F.col("context")["version"].alias("version"),
        F.col("context")["region"].alias("region"),
    )
)
