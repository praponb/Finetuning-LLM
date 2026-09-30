from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") < 2800) & (F.col("revenue") > 2600)) | F.col("store_id").isin([3, 4, 6]))
    .withColumn("units_whole", F.floor("units"))
)
