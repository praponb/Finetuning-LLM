from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.shipping_fee") <= 26) & (F.col("o.channel") == "store"))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
