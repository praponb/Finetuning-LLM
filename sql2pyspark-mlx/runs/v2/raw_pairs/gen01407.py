from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .select(
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("oi.order_id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
    )
    .orderBy(F.col("stock_qty").desc())
)
