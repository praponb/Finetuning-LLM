from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.quantity") < 5)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("oi.order_id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
    )
)
