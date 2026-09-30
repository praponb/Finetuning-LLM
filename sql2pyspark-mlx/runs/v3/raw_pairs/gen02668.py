from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(F.countDistinct("price").alias("unique_brands"))
    .filter(F.col("unique_brands") > 9)
    .select("category", "unique_brands")
    .orderBy("category", F.col("unique_brands").desc())
)
