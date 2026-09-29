from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").isNotNull())
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") < 5)
    .orderBy(F.col("rating_count").desc(), "rating_bucket")
    .limit(5)
)
