from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("sqft") == 4513) & F.col("sqft").between(7277, 8426)) | (F.col("city") == "Tokyo"))
    .withColumn("store_name_prefix", F.substring("store_name", 1, 4))
    .withColumn("sqft_whole", F.ceil("sqft"))
    .orderBy("store_name_prefix")
)
