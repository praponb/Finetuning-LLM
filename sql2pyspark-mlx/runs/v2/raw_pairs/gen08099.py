from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & F.col("store_id").isin([3, 4]))
    .withColumn("quantity_level", F.when(F.col("quantity") >= 239, "High").when(F.col("quantity") >= 66, "Medium").otherwise("Low"))
)
