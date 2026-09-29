from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.shipping_fee") >= 13) & F.col("oi.product_id").isin([16, 26, 27, 34]))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.unit_price"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
    .orderBy(F.col("channel").desc())
)
