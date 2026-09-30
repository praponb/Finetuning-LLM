from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("%Cent%") & (F.col("s.sqft") != 12316))
    .select(
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("c.credit_limit"),
        F.col("c.age"),
        F.col("s.region"),
        F.col("s.city"),
    )
)
