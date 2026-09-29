from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "London")
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.2, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)
