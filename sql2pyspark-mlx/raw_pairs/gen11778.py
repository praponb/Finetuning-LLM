from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.percentile_approx("rating", 0.5).alias("rating_p50"),
        F.count("*").alias("row_count"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .orderBy(F.col("rating_p50").desc())
)
