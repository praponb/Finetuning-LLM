from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.unit_price") <= 130)
    .select(
        F.col("o.channel"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.round(F.col("o.shipping_fee"), 0).alias("shipping_fee_rounded"),
    )
)
