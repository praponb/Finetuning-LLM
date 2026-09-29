from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
    )
    .filter(F.col("de_rating") >= 27)
)
