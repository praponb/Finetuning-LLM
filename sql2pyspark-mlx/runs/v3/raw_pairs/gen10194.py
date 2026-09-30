from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("is_berlin", F.when(F.col("city") == "Berlin", 1).otherwise(0))
)
