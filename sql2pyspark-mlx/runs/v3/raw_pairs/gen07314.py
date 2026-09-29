from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .filter(F.col("th_count") > 3)
    .orderBy(F.col("unique_citys").desc(), "rating_bucket")
)
