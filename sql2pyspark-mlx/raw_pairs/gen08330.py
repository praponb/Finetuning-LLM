from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5, 6]))
    .withColumn("quantity_rounded", F.round("quantity", 1))
)
