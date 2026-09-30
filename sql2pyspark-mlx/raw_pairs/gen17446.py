from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6]))
    .withColumn("quantity_rounded", F.round("quantity", 2))
    .withColumn("quantity_str", F.col("quantity").cast("string"))
)
