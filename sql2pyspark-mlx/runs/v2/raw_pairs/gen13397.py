from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.discount") <= 0.24)
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.method"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("c.is_active"),
        F.col("c.city"),
    )
    .orderBy("city", "id")
    .limit(5)
)
