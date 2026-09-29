from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 9]))
    .groupBy(F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") < 4)
    .orderBy("unique_products")
)
