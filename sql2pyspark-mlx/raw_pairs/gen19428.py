from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.filter(F.col("product_id") == 6).alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left_semi")
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("p.price"),
    )
    .orderBy("supplier_id")
)
