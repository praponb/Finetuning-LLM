from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.percentile_approx("rating", 0.95).alias("rating_p95_2"),
        F.avg("rating").alias("avg_rating"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles_2"),
    )
    .orderBy("rating_p95_2")
)
