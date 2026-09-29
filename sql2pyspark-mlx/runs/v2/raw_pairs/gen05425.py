from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("city").isin(["Bangkok", "New York"]))

rating_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("total_rating") >= 9)
    .orderBy("rating_bucket")
    .limit(10)
)
