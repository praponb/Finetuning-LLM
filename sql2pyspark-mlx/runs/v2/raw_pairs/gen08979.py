from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "London") & F.col("supplier_name").like("Si%"))
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy(F.col("rating_count").desc(), F.col("stddev_rating").desc())
)
