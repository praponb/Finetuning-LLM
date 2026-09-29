from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.count("sqft").alias("sqft_count"),
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
    )
    .filter(F.col("east_sqft") >= 28)
    .drop("east_sqft")
    .orderBy(F.col("sqft_count").desc(), "city")
    .limit(50)
)
