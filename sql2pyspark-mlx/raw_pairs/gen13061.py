from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("p.status"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("c.name"),
        F.col("c.age"),
    )
    .orderBy("discount", F.col("age").desc())
)
