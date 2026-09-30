from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)
