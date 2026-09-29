from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
    )
)
