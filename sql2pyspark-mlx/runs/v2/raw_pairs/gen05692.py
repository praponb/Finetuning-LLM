from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.order_id").isin([5, 31]))
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.brand"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
