from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("o.channel"),
        F.col("c.segment"),
        F.col("c.age"),
        (F.col("o.amount") + F.col("o.shipping_fee")).alias("amount_with_shipping"),
    )
    .orderBy(F.col("age").desc(), "channel")
)
