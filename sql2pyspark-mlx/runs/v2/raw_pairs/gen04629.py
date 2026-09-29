from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "status",
        "amount",
        F.substring("channel", 1, 2).alias("channel_prefix"),
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
)
