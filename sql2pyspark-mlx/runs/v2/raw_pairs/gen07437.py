from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([6, 7, 8]))
    .groupBy("store_id", "product_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") < 130)
    .orderBy(F.col("product_id").desc())
)
