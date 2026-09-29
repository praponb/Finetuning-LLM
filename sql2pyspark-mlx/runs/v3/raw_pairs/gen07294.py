from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 15216, "high").when(F.col("sqft") >= 4892, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
    .filter(F.col("stddev_sqft") >= 34)
    .select("sqft_bucket", "stddev_sqft")
    .orderBy(F.col("stddev_sqft").desc())
)
