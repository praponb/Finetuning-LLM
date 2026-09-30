from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.lower(F.col("p.category")).alias("category_lower"),
    )
)
