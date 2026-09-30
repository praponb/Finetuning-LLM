from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "South")
    .groupBy("region")
    .agg(F.min("sqft").alias("min_sqft"))
    .orderBy("region")
    .limit(5)
)
