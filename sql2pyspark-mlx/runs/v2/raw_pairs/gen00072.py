from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.sum("rating").alias("total_rating"),
    )
    .orderBy(F.col("total_rating").desc())
)
