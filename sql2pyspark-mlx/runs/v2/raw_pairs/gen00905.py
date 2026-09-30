from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-10-17", "2025-09-21"))
    .groupBy("region", "city")
    .agg(F.count("sqft").alias("sqft_count"))
    .filter(F.col("sqft_count") > 6)
    .select("region", "city", "sqft_count")
    .orderBy(F.col("sqft_count").desc(), "region", "city")
    .limit(10)
)
