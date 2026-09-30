from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(((F.col("country") == "UK") & F.col("country").isin(["US", "TH"])) | F.col("city").isin(["Tokyo", "London"]))
)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.avg("rating").alias("avg_rating"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    rating_bucket_summary
    .select("rating_bucket", "avg_rating", "max_rating")
    .orderBy("rating_bucket", "max_rating")
)
