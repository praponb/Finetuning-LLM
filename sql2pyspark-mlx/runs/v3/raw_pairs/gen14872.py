from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.customer_id"),
    )
)
