from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 3, 4, 6]) & (F.col("product_id").isin([1, 5, 8, 9]) | (F.col("last_restock") > "2023-09-01"))))
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
)
