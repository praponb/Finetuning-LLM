from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") >= 1)
    .drop("unique_citys")
    .orderBy(F.col("stddev_sqft").desc(), "city", "region")
    .limit(3)
)
