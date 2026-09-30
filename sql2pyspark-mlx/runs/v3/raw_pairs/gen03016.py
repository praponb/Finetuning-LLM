from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "COMPLETED") & F.col("store_id").isin([30, 33, 37]))
    .select(
        "id",
        "shipping_fee",
        F.coalesce("channel", F.lit("Unknown")).alias("channel_filled"),
    )
)
