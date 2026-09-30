from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.store_id"),
        F.col("c.city"),
        F.col("c.email"),
    )
    .orderBy("city")
)
