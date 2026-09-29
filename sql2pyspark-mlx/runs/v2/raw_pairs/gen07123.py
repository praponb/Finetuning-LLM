from pyspark.sql import functions as F

result = (
    df_suppliers
    .rollup(
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 1.6, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
