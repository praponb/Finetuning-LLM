from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") <= 13992)
    .groupBy("city", "region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .orderBy("city")
)
