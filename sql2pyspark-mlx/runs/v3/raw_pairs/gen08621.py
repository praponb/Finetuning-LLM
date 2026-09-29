from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("oi.order_id"),
    )
    .orderBy(F.col("store_id").desc())
)
