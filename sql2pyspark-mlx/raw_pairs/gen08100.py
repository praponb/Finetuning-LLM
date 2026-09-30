from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
    .orderBy(F.col("avg_rating").desc())
)
