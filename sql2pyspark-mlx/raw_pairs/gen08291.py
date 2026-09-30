from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull())
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("o.store_id"),
        F.col("c.city"),
        F.col("c.credit_limit"),
    )
)
