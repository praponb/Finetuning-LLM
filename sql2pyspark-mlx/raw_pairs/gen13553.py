from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("unit_price") > 120).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.col("o.status") == "PENDING")
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
    .orderBy("status")
)
