from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 10]))
    .agg(F.avg("quantity").alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
