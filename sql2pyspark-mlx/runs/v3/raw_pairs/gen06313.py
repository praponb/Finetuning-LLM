from pyspark.sql import functions as F

product_summary = (
    df_store_sales
    .groupBy("product_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    df_products.alias("p")
    .join(product_summary.alias("t"), F.col("p.id") == F.col("t.product_id"), "inner")
    .filter(F.col("t.unique_stores") >= 9)
    .select(F.col("p.price"), F.col("t.unique_stores"))
    .orderBy("price")
)
