from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.6)
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_citys"))
    .filter(F.col("unique_citys") < 8)
    .orderBy(F.col("unique_citys").desc(), "rating_bucket")
    .limit(10)
)
