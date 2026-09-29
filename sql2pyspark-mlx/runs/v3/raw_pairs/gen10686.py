from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 1.9)
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.4, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") > 5)
    .select("rating_bucket", "city", "rating_count")
    .orderBy("city", "rating_count")
)
