from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") >= 72)
    .orderBy("city", F.col("unique_citys").desc())
    .limit(1)
)
