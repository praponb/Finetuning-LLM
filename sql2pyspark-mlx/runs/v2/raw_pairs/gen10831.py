from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("country") != "US")

suppliers_stats = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("singapore_rating") >= 44)
    .select("rating_bucket", "city", "max_rating")
)
