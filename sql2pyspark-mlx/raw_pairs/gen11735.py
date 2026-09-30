from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.price").between(920, 1100))
    .groupBy(F.col("p.id"), F.col("p.category"))
    .agg(F.count(F.col("ss.id")).alias("store_sale_count"))
    .orderBy("category", "store_sale_count")
)
