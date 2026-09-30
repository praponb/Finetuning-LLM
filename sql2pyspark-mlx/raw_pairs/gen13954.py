from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.least(F.col("o.shipping_fee"), F.col("o.discount")).alias("least_shipping_fee_discount"),
    )
)
