from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.order_id").isin([7, 30, 35, 36]))
    .select(
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
    )
)
