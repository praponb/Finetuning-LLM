from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isNull())
    .select(
        F.col("o.amount"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("c.country"),
    )
)
