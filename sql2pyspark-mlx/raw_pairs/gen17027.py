from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.status") == "COMPLETED")
    .select(
        F.col("o.channel"),
        F.col("o.status"),
        F.col("c.name"),
        F.col("s.region"),
        F.col("s.sqft"),
    )
)
