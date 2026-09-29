from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("rating_tier", F.when(F.col("rating") >= 3.9, "PREMIUM").otherwise("STANDARD"))
    .cube("city", "rating_tier")
    .agg(
        F.stddev_samp("rating").alias("rating_stddev"),
        F.max(F.when(F.col("city") == "Berlin", F.col("rating"))).alias("berlin_max_rating"),
        F.stddev_samp("rating").alias("rating_stddev_2"),
        F.grouping("city").alias("grouping_level"),
    )
)
