from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("s.city"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
    .orderBy("shipping_fee")
)
