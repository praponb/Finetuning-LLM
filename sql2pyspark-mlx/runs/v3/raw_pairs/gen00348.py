from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "SG")
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("rating")).otherwise(0)).alias("new_york_rating"),
    )
    .filter(F.col("new_york_rating") < 54)
)
