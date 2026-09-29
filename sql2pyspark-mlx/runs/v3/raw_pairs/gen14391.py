from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") < 1.9)
    .orderBy("rating_bucket")
)
