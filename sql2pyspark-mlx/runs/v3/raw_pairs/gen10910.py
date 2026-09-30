from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "West") & (F.col("city") == "Bangkok"))
    .groupBy(
        F.when(F.col("sqft") >= 15504, "high").when(F.col("sqft") >= 4378, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)
