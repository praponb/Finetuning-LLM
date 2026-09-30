from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("oi.product_id").isin([5, 6, 10, 40]))
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
    .orderBy("credit_limit", F.col("segment").desc())
)
