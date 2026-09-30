from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter((F.col("o.discount") >= 0.12) & (F.year(F.col("o.order_date")) == 2023))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
)
