from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy("rating_bucket", "country")
    .limit(5)
)
