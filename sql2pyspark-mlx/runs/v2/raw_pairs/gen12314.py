from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("oi.unit_price") >= 240) & (F.col("p.launch_date") > "2023-10-01"))
    .select(
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.brand"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.lower(F.col("p.category")).alias("category_lower"),
    )
    .orderBy(F.col("product_id").desc(), "brand")
)
