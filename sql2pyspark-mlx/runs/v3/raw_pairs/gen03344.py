from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["Singapore", "New York"]) & (F.col("rating") > 2.6))
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
    )
)
