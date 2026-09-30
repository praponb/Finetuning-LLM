from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
    )
    .orderBy(F.col("jp_count").desc(), "city")
)
