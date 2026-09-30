from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
    )
    .filter(F.col("uk_rating") >= 68)
    .select("rating_bucket", "uk_rating")
    .orderBy(F.col("uk_rating").desc(), "rating_bucket")
    .limit(1)
)
