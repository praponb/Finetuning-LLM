from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(1.8, 4.1))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
    )
    .filter(F.col("chiang_mai_rating") < 36)
    .select("city", "rating_bucket", "chiang_mai_rating", "de_rating")
    .orderBy("city", "rating_bucket")
    .limit(50)
)
