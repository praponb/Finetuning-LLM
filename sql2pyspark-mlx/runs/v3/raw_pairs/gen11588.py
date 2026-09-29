from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.length(F.col("s.store_name")) >= 14)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("s.region"),
        F.col("s.sqft"),
        F.length(F.col("o.status")).alias("status_length"),
    )
)
