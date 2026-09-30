from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("p.order_id").isin([3, 12, 20, 40]) & (F.col("p.paid_at") >= "2024-07-01"))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.amount"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("c.age"),
    )
)
