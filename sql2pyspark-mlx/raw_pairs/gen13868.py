from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.price") <= 440)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.supplier_id"),
        F.col("p.category"),
    )
)
