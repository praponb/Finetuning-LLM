from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count("rating").alias("rating_count"))
    .filter(F.col("rating_count") >= 4)
    .orderBy("city")
)
