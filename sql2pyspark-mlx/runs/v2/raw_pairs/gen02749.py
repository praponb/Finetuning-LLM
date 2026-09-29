from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.order_date") <= "2023-09-26") & F.col("c.age").between(66, 67))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("c.country"),
        F.col("c.city"),
    )
)
