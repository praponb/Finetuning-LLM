from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 38)
    .select(
        "id",
        "channel",
        F.coalesce("shipping_fee", F.lit(0)).alias("shipping_fee_filled"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)
