from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.count("rating").alias("rating_count"),
    )
    .orderBy("rating_bucket", "city")
    .limit(5)
)
