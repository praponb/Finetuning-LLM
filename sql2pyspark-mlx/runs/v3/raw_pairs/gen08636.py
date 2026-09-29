from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "JP") | ~F.col("city").isin(["Berlin", "Tokyo"]))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count("rating").alias("rating_count"))
    .orderBy(F.col("rating_count").desc(), "country", "rating_bucket", "city")
    .limit(50)
)
