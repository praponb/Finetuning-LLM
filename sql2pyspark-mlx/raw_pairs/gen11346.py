from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("rating").between(2.9, 3.1))
    .withColumn("rating_filled", F.coalesce("rating", F.lit(0)))
)

summary = (
    src
    .groupBy(
        F.when(F.col("rating_filled") >= 73, "high").when(F.col("rating_filled") >= 31, "mid").otherwise("low").alias("rating_filled_bucket"),
        "city",
    )
    .agg(F.min("rating_filled").alias("min_rating_filled"))
)

result = (
    summary
    .filter(F.col("min_rating_filled") < 71)
    .select("rating_filled_bucket", "min_rating_filled")
    .orderBy(F.col("min_rating_filled").desc(), "rating_filled_bucket")
)
