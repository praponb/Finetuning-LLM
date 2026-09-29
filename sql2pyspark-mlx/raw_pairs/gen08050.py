from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.unit_price") < 190)
    .select(
        F.col("oi.unit_price"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
)
