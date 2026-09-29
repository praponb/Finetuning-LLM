from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
    )
    .filter(F.col("tokyo_rating") > 66)
    .select("rating_bucket", "country", "tokyo_rating")
)
