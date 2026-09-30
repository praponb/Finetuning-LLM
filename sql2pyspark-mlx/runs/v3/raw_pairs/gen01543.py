from pyspark.sql import functions as F

product_totals = (
    df_store_sales
    .filter(F.col("units").between(15, 30))
    .groupBy("product_id")
    .agg(F.count("revenue").alias("revenue_count"))
)

result = (
    df_products.alias("products")
    .join(product_totals.alias("t"), F.col("products.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.revenue_count") >= 5)
    .select(F.col("products.id"), F.col("products.category"), F.col("t.revenue_count"))
    .orderBy(F.col("revenue_count").desc())
)
