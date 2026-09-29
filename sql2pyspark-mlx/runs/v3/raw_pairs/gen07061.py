from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(F.countDistinct("quantity").alias("unique_products"))
    .orderBy(F.col("store_id").desc())
)
