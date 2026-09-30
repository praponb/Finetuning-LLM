from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "category")
    .agg(F.countDistinct("price").alias("unique_brands"))
    .filter(F.col("unique_brands") > 1)
    .orderBy("unique_brands")
)
