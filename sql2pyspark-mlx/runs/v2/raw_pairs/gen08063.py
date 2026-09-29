from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.count("rating").alias("rating_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") > 3)
    .drop("tokyo_count")
    .orderBy(F.col("rating_count").desc(), "country")
)
