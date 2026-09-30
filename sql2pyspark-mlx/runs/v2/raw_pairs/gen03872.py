from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") != "Berlin")
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
    .orderBy("country")
)
