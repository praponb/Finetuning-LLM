from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.length(F.col("c.name")) > 11)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("c.email"),
    )
    .orderBy("amount")
)
