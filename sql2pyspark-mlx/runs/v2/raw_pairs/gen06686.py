from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "East"]))
    .groupBy("region", "city")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("unique_citys") > 4)
    .orderBy("unique_citys", "total_sqft", "region", "city")
    .limit(3)
)
