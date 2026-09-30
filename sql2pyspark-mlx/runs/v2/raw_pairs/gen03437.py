from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 9)
    .select("city", "sqft_count")
    .orderBy("sqft_count", "city")
    .limit(3)
)
