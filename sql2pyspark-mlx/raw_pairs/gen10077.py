from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.country") == "UK")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("o.store_id"),
        F.col("c.country"),
        F.col("c.name"),
    )
)
