from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isNotNull() & (F.col("o.shipping_fee") <= 26))
    .select(
        F.col("o.amount"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.product_id"),
    )
)
