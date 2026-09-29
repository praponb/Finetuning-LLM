from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.2, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.countDistinct("rating").alias("unique_citys"))
    .orderBy("city")
)
