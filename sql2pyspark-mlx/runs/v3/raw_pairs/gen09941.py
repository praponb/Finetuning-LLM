from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.col("revenue") >= 2100)
    .groupBy("product_id")
    .agg(F.max("sale_date").alias("latest_sale_date"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .select(F.col("p.brand"), F.col("p.supplier_id"), F.col("t.latest_sale_date"))
)
