from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.1)
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.countDistinct("rating").alias("unique_citys"),
    )
    .filter(F.col("rating_range") >= 76)
)
