from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "SG", "JP"]) & F.col("supplier_name").isNotNull())
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.count("rating").alias("rating_count"),
    )
    .orderBy(F.col("rating_bucket").desc())
)
