from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("region") == "Central") & (F.col("sqft") != 13004)) | F.col("sqft").isNotNull())
    .groupBy("city")
    .agg(F.max("sqft").alias("max_sqft"))
    .filter(F.col("max_sqft") < 10200)
    .orderBy("city")
)
