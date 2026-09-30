from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.quantity") > 8)
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("stock_qty").desc())
)
