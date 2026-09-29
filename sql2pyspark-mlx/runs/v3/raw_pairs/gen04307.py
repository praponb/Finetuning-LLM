from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 2.3, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count(F.when(F.col("city") == "London", 1)).alias("london_count"))
    .orderBy("rating_bucket")
)
