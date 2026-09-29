from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(F.avg(F.col("ss.revenue")).alias("avg_revenue"))
    .orderBy(F.col("avg_revenue").desc(), "category", "brand")
    .limit(10)
)
