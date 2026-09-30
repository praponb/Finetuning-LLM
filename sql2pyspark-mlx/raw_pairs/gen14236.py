from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 4.2, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.stddev_samp("rating").alias("rating_stddev_2"),
        F.avg("rating").alias("avg_rating"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("rating_per_singapore"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .orderBy("rating_stddev_2")
)
