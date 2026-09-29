from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter((F.col("oi.quantity") >= 7) & (F.col("oi.unit_price") < 310))
    .select(
        F.col("p.stock_qty"),
        F.col("p.supplier_id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
    )
    .orderBy("product_id")
)
