from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["London", "Tokyo", "Singapore"]))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .filter(F.col("avg_rating") >= 26)
    .orderBy("avg_rating", "rating_bucket")
    .limit(3)
)
