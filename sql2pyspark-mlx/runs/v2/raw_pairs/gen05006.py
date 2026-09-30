from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)
