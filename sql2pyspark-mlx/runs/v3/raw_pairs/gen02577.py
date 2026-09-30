from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.5)
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
    .filter(F.col("min_rating") > 2.8)
    .orderBy("rating_bucket", "country")
    .limit(10)
)
