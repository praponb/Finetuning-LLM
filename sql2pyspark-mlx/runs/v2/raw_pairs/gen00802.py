from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .filter(F.col("avg_rating") >= 30)
    .orderBy("rating_bucket")
)
