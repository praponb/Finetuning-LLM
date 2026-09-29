from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "inner")
    .filter(F.col("p.category").isin(["Home", "Books"]))
    .select(
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("p.category"),
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
    )
)
