from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") >= 3)
    .select("region", "sqft_count")
    .orderBy(F.col("sqft_count").desc())
)
