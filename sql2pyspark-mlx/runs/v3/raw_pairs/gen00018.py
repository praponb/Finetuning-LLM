from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.discount"),
        F.col("o.amount"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
    .orderBy(F.col("amount").desc())
)
