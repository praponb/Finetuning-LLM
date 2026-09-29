from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.status").isin(["PENDING", "COMPLETED"]))
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("c.email"),
    )
    .orderBy(F.col("quantity").desc(), "order_id")
)
