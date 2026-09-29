from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

rating_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.max("rating_filled").alias("max_rating_filled"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("max_rating_filled") >= 42)
    .orderBy("rating_range", F.col("rating_bucket").desc())
)
