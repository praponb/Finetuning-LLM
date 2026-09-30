from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") >= 7)
    .select("store_id", "unique_products")
)
