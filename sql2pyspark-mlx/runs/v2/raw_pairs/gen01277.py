from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 5]) & (F.col("quantity") >= 185))
    .groupBy("store_id")
    .agg(F.min("quantity").alias("min_quantity"))
    .filter(F.col("min_quantity") >= 80)
    .orderBy("min_quantity")
)
