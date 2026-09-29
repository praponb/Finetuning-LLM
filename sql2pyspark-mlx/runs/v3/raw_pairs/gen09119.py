from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.sqft") == 8080)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("s.city"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
)
