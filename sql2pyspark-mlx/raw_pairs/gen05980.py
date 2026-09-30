from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.filter(F.col("region") == "Central").alias("s"), F.col("o.store_id") == F.col("s.id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
    .orderBy("order_date")
)
