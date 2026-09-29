from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("s.cost"),
        F.col("s.order_id"),
        F.col("s.status"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
    )
    .orderBy("store_id")
)
