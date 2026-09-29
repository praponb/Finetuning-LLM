from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") >= "2024-03-01")
    .groupBy(
        F.when(F.col("sqft") >= 14580, "high").when(F.col("sqft") >= 7197, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") < 65)
)
