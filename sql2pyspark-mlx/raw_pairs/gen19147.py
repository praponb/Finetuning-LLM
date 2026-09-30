from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") <= 1180)
    .groupBy("supplier_id")
    .agg(F.countDistinct("price").alias("unique_brands"))
    .filter(F.col("unique_brands") >= 8)
    .select("supplier_id", "unique_brands")
    .orderBy("supplier_id")
    .limit(50)
)
