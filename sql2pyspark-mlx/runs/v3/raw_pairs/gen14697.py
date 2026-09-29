from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 6)
    .withColumn("quantity_band", F.when(F.col("quantity") >= 238, "High").when(F.col("quantity") >= 117, "Medium").otherwise("Low"))
    .orderBy("quantity_band")
)
