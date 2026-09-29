from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price") < 200).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.status"),
    )
    .orderBy("store_id", F.col("id").desc())
    .limit(10)
)
