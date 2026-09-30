from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") >= 1.9) | (F.col("rating") > 4.0))
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.min("rating").alias("min_rating"),
    )
)
