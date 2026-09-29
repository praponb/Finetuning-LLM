from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("o.store_id"),
        F.col("o.discount"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
    )
    .orderBy(F.col("discount").desc(), "id")
    .limit(1)
)
