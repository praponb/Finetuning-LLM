from pyspark.sql import functions as F

result = (
    df_orders
    .filter(((F.col("shipping_fee") < 26) & (F.col("order_date") <= "2024-09-01")) | F.col("store_id").isNull())
    .select(
        "amount",
        "channel",
        "status",
        (F.col("shipping_fee") - F.col("amount")).alias("amount_with_shipping"),
    )
    .orderBy("status")
)
