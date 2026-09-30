from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 7]))
    .groupBy("store_id")
    .agg(F.count("quantity").alias("quantity_count"))
    .orderBy(F.col("store_id").desc())
)
