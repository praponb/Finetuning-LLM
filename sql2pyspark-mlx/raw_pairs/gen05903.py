from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.shipping_fee").isNotNull())
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("c.city"),
        F.col("c.age"),
        F.col("s.city").alias("store_city"),
        F.col("s.region"),
    )
)
