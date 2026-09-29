from pyspark.sql import functions as F

result = (
    df_app_logs
    .select(
        "id",
        "service",
        "context",
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        (F.col("status_code").bitwiseAND(2) != 0).cast("int").alias("flag_bit2"),
    )
)
