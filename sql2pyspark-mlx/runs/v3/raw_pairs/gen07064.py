from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]) & (F.col("product_id") == 5) & F.col("product_id").isin([3, 4, 6, 10]))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 214, "High").when(F.col("quantity") >= 73, "Medium").otherwise("Low"))
)
