from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.amount") > 780)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("c.city"),
        F.col("c.email"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.upper(F.col("c.segment")).alias("segment_upper"),
    )
)
