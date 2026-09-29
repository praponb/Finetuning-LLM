from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 16129, "high").when(F.col("sqft") >= 2491, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)
