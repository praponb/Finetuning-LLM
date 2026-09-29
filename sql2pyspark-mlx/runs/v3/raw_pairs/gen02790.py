from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter((F.col("rating").between(2.1, 3.3) & ~F.col("city").isin(["Bangkok", "New York", "London"])) | F.col("country").isin(["JP", "US", "UK"]))
    .withColumn("rating_rounded", F.round("rating", 0))
)

rating_rounded_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("rating_rounded") >= 81, "high").when(F.col("rating_rounded") >= 19, "mid").otherwise("low").alias("rating_rounded_bucket"),
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
)

result = rating_rounded_bucket_summary.select("rating_rounded_bucket", "country_list")
