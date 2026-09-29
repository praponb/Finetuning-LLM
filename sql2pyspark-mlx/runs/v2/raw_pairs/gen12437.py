from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.sum(F.when(F.col("country") == "US", F.col("rating")).otherwise(0)).alias("us_rating"),
    )
    .filter(F.col("us_rating") < 67)
)
