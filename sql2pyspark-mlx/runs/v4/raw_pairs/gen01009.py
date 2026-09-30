from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(145, 186))
    .withColumn("quantity_adjusted", F.round((F.col("quantity") * 1.07), 2))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
    .orderBy("quantity_adjusted", F.col("store_id").desc())
)
