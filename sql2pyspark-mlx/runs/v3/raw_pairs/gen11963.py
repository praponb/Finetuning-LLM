from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.status"),
        F.col("c.name"),
        F.col("c.segment"),
    )
)
