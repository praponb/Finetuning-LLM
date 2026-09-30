from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("t.last_sale_date"),
    )
    .orderBy("category", "id")
    .limit(50)
)
