from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .filter(F.col("uk_count") > 3)
)
