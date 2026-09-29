from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.amount") <= 1400)
    .select(
        F.col("o.amount"),
        F.col("c.email"),
        F.col("c.segment"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
