from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("status_code") <= 339)
    .select(
        "id",
        "status_code",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
        F.col("context")["trace"].alias("trace"),
    )
)
