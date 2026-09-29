from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.product_id").isin([29, 37]))
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("o.discount"),
        F.col("o.status"),
    )
    .orderBy(F.col("id").desc())
)
