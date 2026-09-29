from pyspark.sql import functions as F

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "started_at",
        "device",
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
    )
)
