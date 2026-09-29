from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.2)
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.countDistinct("rating").alias("unique_countrys"),
        F.min("rating").alias("min_rating"),
    )
    .orderBy("country")
)
