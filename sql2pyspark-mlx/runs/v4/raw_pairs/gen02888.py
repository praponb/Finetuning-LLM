from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.shipping_fee").between(19, 20))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.status"),
        F.col("o.channel"),
        F.col("s.region"),
    )
)
