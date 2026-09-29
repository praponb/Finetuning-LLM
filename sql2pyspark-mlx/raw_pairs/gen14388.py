from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16642, "high").when(F.col("sqft") >= 7538, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
        F.min("opened_date").alias("min_opened_date"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy(F.col("sqft_bucket").desc())
)
