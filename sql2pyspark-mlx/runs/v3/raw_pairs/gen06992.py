from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.channel") == "web")
    .select(
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.status").alias("order_status"),
        F.col("c.age"),
        F.col("c.country"),
    )
)
