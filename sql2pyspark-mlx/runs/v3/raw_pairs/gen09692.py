from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .groupBy("city")
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") < 5)
    .orderBy("sqft_count", "city")
    .limit(20)
)
