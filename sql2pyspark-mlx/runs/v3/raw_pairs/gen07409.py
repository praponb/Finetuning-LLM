from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") == 9296) | (F.col("o.store_id") == 14))
    .select(
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("c.city"),
        F.col("c.email"),
        F.col("s.city").alias("store_city"),
    )
    .orderBy(F.col("email").desc())
)
