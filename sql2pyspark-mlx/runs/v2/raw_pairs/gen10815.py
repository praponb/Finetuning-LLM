from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .filter(F.col("o.discount") >= 0.32)
    .select(
        F.col("o.customer_id"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("c.city").alias("customer_city"),
        F.col("c.name"),
    )
)
