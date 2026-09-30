from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .withColumn("rating_rounded", F.round("rating", 0))
)

rating_rounded_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("rating_rounded") >= 75, "high").when(F.col("rating_rounded") >= 37, "mid").otherwise("low").alias("rating_rounded_bucket"),
    )
    .agg(F.round(F.stddev("rating_rounded"), 2).alias("stddev_rating_rounded"))
)

result = (
    rating_rounded_bucket_summary
    .filter(F.col("stddev_rating_rounded") > 74)
    .select("rating_rounded_bucket", "stddev_rating_rounded")
    .orderBy("stddev_rating_rounded", "rating_rounded_bucket")
    .limit(3)
)
