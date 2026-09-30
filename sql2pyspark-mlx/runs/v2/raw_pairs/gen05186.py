from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
)
