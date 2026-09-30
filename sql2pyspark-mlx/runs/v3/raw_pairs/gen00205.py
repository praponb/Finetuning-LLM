from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") == 78)
    .groupBy(F.year("last_restock").alias("last_restock_year"))
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy(F.col("unique_products").desc(), F.col("last_restock_year").desc())
    .limit(10)
)
