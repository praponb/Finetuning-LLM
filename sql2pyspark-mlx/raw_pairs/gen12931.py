from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3]))
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy(F.col("store_id").desc())
    .limit(5)
)
