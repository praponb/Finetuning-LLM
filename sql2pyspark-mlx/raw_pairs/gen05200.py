from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .groupBy("category", "supplier_id")
    .agg(F.countDistinct("cost").alias("unique_brands"))
    .filter(F.col("unique_brands") > 5)
    .orderBy("category")
)
