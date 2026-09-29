from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", "region")
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.countDistinct("sqft").alias("unique_citys"),
    )
    .filter(F.col("min_sqft") > 15200)
    .orderBy("min_sqft", F.col("unique_citys").desc())
)
