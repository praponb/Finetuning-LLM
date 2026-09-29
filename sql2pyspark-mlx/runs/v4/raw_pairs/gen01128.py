from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("order_id").isin([12, 19, 22, 34])).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_anti")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.status"),
        F.col("o.shipping_fee"),
    )
    .orderBy("store_id")
)
