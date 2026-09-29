from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.countDistinct("cost").alias("unique_brands"))
    .filter(F.col("unique_brands") < 4)
    .select("brand", "unique_brands")
    .orderBy("unique_brands")
)
