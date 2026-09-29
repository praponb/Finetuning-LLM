from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        "city",
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("rating").alias("rating_count"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
    )
)
