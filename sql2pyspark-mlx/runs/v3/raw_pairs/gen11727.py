from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.store_id").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("o.status"),
        F.col("c.is_active"),
        F.col("c.email"),
    )
)
