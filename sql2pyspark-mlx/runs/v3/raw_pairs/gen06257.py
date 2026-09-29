from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 12332, "high").when(F.col("sqft") >= 3971, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)
