from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") > 12849)
    .withColumn("store_name_lower", F.lower("store_name"))
)
