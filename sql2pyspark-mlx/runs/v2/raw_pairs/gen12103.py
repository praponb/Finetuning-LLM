from pyspark.sql import functions as F

product_stats = (
    df_order_items
    .groupBy("product_id")
    .agg(F.max("unit_price").alias("max_unit_price"))
)

result = (
    df_products.alias("products")
    .join(product_stats.alias("product_stats"), F.col("products.id") == F.col("product_stats.product_id"), "inner")
    .select(
        F.col("products.category"),
        F.col("products.supplier_id"),
        F.col("product_stats.max_unit_price"),
    )
    .orderBy(F.col("supplier_id").desc())
)
