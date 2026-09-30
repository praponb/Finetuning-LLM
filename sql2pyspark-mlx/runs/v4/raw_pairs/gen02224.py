from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 14092, "high").when(F.col("sqft") >= 7807, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
    .orderBy("stddev_sqft")
)
