from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "West", 1).otherwise(0)).alias("west_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .filter(F.col("sqft_range") >= 50)
    .select("city", "west_count", "sqft_range")
    .orderBy("city", F.col("sqft_range").desc())
)
