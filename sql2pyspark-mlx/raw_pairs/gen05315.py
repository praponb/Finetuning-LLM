from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.customer_id").isin([7, 23, 31, 34]))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("s.city").alias("store_city"),
        F.col("s.region"),
    )
)
