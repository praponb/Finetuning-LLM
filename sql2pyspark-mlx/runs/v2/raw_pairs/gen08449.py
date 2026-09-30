from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 2.6) & F.col("city").isNull())
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.countDistinct("rating").alias("unique_citys"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
    )
    .filter(F.col("us_rating") > 55)
    .drop("us_rating")
    .orderBy(F.col("unique_citys").desc(), "total_rating")
)
