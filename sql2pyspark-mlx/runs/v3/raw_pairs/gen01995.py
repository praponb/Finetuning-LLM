from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.length(F.col("c.email")) < 5)
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("c.email"),
        F.col("c.country"),
    )
    .orderBy("channel")
)
