from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("p.supplier_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("oi.quantity"),
        F.col("oi.quantity").cast("double").alias("quantity_num"),
    )
    .orderBy(F.col("quantity").desc())
)
