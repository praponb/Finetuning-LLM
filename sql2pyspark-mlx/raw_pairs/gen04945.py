from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("s.city"),
    )
    .orderBy("shipping_fee")
)
