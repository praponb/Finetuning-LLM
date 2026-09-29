from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 4)
    .groupBy("product_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .filter(F.col("unique_products") >= 5)
    .orderBy(F.col("unique_products").desc())
)
