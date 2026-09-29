from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
    .orderBy(F.col("credit_limit").desc(), "order_id")
)
