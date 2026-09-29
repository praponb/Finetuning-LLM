from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.product_id").isin([7, 37, 38, 40]) & F.col("oi.order_id").isin([3, 9, 19, 35]))
    .select(
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
        F.col("o.amount"),
    )
)
