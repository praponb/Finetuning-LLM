from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(~((F.year(F.col("o.order_date")) >= 2025) & (F.col("oi.order_id") == 33)))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("c.name"),
    )
)
