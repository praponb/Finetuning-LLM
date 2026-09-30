from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("oi.product_id") == 6)
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("c.age"),
        F.col("c.is_active"),
    )
)
