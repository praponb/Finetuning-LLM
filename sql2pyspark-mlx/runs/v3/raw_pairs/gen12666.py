from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 9]) & (F.col("last_restock") > "2024-03-01"))
    .withColumn("quantity_whole", F.ceil("quantity"))
)
