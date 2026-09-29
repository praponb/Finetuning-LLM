from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("unique_citys") > 9)
    .select("rating_bucket", "unique_citys", "max_rating")
    .orderBy(F.col("unique_citys").desc(), F.col("max_rating").desc())
)
