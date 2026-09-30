from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.avg("revenue").alias("avg_revenue"))
)

result = (
    df_products.alias("p")
    .join(store_sales_by_product.alias("S"), F.col("p.id") == F.col("S.product_id"), "left")
    .filter(F.col("S.avg_revenue") >= 1600)
    .select(F.col("p.id"), F.col("p.brand"), F.col("S.avg_revenue"))
    .orderBy("avg_revenue", F.col("id").desc())
)
