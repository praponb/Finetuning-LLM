from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("rating")).otherwise(0)).alias("chiang_mai_rating"),
        F.count("rating").alias("rating_count"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("total_rating") > 20)
    .orderBy("rating_bucket")
)
