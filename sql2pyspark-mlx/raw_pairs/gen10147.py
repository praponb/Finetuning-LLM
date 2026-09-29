from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("city") == "Singapore")

rating_bucket_summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)

result = rating_bucket_summary.select("rating_bucket", "city", "stddev_rating")
