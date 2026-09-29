from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("rating_per_bangkok"),
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("rating_per_de"),
    )
    .filter(F.col("num_countries") < 1)
)
