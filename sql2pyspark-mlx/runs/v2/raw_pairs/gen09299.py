from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("o.store_id"),
    )
)
