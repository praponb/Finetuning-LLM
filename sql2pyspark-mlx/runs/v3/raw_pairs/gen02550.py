from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(~(F.col("oi.order_id").isin([9, 10, 36]) & F.col("o.store_id").isNull()))
    .select(
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("oi.order_id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
    )
)
