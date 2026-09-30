from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.store_id").isNull() & (F.col("oi.quantity") == 6))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.amount"),
        F.col("c.name"),
        F.col("c.age"),
    )
)
