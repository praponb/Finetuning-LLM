from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("order_id").isin([7, 32])).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .filter(F.col("p.price") <= 890)
    .select(
        F.col("p.id"),
        F.col("p.brand"),
        F.col("p.supplier_id"),
        F.col("p.category"),
    )
)
