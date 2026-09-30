from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Tokyo"]) & (F.col("country") != "UK"))
    .groupBy(
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("uk_rating").desc(), "rating_bucket")
)
