from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.avg("rating").alias("avg_rating"))
    .filter(F.col("avg_rating") < 3.0)
    .select("rating_bucket", "city", "avg_rating")
)
