from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 17492, "high").when(F.col("sqft") >= 4646, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.count(F.when(F.col("region") == "South", 1)).alias("south_count"))
)
