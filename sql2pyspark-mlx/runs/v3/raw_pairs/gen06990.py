from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 213)
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
    .filter(F.col("avg_quantity") > 120)
)
