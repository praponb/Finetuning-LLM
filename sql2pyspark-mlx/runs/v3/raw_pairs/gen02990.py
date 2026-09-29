from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.channel") == "store")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("c.email"),
        F.col("c.credit_limit"),
    )
)
