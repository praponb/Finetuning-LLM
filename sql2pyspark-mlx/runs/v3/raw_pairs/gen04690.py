from pyspark.sql import functions as F

result = (
    df_sessions
    .select(
        "device",
        "customer_id",
        "started_at",
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
    )
)
