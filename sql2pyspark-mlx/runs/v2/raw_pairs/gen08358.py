from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
    .filter(F.col("min_rating") > 3.9)
    .select("country", "rating_bucket", "min_rating")
)
