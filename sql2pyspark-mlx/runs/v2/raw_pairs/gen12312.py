from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("c.country").isin(["US", "DE"]))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("c.city"),
        F.col("s.sqft"),
        F.col("s.store_name"),
    )
)
