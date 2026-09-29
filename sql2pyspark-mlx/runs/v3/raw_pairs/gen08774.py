from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_count"),
    )
    .filter(F.col("west_count") > 9)
    .select("city", "region", "sqft_count", "west_count")
    .orderBy("sqft_count", F.col("west_count").desc())
)
