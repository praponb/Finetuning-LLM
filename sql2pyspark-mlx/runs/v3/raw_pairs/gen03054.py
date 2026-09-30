from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12454, "high").when(F.col("sqft") >= 4576, "mid").otherwise("low").alias("sqft_bucket"),
        F.month("opened_date").alias("opened_month"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "East", F.col("sqft")).otherwise(0)).alias("east_sqft"),
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
    )
    .orderBy(F.col("east_sqft").desc())
)
