from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
        "country",
        "city",
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("max_rating") >= 3.4)
)
