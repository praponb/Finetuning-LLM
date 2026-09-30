from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country") != "UK")

city_summary = (
    recent_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
)

result = (
    city_summary
    .filter(F.col("total_rating") >= 14)
    .select("city", "rating_bucket", "total_rating")
    .orderBy("rating_bucket", F.col("total_rating").desc())
)
