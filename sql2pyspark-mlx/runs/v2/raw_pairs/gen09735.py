from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "country",
        "city",
    )
    .agg(F.sum("rating").alias("total_rating"))
    .orderBy("country", "rating_bucket", "city")
    .limit(1)
)
