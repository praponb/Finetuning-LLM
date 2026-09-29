from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.customer_id").isNotNull() & (F.col("o.amount") > 690))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("s.sqft"),
        F.col("c.age"),
    )
)
