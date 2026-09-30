from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("oi.order_id").isin([12, 17]) & F.col("oi.product_id").isNotNull())
    .select(
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.stock_qty"),
    )
)
