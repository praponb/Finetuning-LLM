from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 17063, "high").when(F.col("sqft") >= 5933, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") >= 9)
    .orderBy(F.col("avg_sqft").desc())
)
