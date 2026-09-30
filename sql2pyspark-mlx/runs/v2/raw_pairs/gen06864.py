from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 5, 6]) & (F.col("product_id") == 3) & F.col("store_id").isin([1, 2, 4, 5]))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 184, "High").when(F.col("quantity") >= 112, "Medium").otherwise("Low"))
)
