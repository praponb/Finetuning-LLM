from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.9, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
    )
    .orderBy(F.col("bangkok_rating").desc_nulls_last())
)
