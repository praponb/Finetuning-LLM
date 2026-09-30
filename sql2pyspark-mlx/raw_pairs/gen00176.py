from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("rating")).otherwise(0)).alias("berlin_rating"),
    )
    .filter(F.col("berlin_rating") > 42)
)
