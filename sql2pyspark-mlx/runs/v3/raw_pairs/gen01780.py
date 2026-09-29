from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 5)
    .withColumn("quantity_level", F.when(F.col("quantity") >= 197, "High").when(F.col("quantity") >= 31, "Medium").otherwise("Low"))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
    .orderBy(F.col("id").desc())
    .limit(50)
)
