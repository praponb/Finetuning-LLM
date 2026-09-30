from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.age"),
    )
    .orderBy(F.col("age").desc(), "email")
)
