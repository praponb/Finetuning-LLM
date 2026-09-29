from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("c.segment"),
        F.col("s.sqft"),
    )
)
