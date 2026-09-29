from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 13256, "high").when(F.col("sqft") >= 4428, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .filter(F.col("stddev_sqft") >= 77)
    .orderBy("stddev_sqft", "sqft_bucket")
)
