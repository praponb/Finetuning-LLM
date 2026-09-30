from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 2.6)
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)
