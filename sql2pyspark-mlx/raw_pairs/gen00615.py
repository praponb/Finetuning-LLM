from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 12300, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        "city",
    )
    .agg(
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.stddev_samp("sqft").alias("sqft_stddev_2"),
        F.avg("sqft").alias("avg_sqft_2"),
        F.countDistinct("city").alias("distinct_cities"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
    .filter(F.col("distinct_cities") >= 1)
    .orderBy(F.col("city").asc_nulls_last())
)
