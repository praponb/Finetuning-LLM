from pyspark.sql import functions as F

result = (
    df_app_logs
    .filter(F.col("status_code").between(272, 305))
    .select(
        "id",
        "status_code",
        F.col("context")["trace"].alias("trace"),
        F.coalesce(F.col("context").getItem("version"), F.lit("none")).alias("version_or_default"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
    )
)
