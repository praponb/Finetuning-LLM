from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 7, 8]) & (F.col("quantity") == 230))
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy("avg_quantity", "product_id")
)
