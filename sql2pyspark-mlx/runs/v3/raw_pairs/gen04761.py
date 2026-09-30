from pyspark.sql import functions as F

product_stats = (
    df_store_sales
    .filter(F.month("sale_date").isin([3, 4, 12]))
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(product_stats.alias("ps"), F.col("p.id") == F.col("ps.product_id"), "inner")
    .filter(F.col("ps.unique_stores") > 10)
    .select(F.col("p.price"), F.col("p.product_name"), F.col("ps.unique_stores"))
)
