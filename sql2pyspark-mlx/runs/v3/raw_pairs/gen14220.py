from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("quantity_level", F.when(F.col("quantity") >= 210, "High").when(F.col("quantity") >= 69, "Medium").otherwise("Low"))
)
