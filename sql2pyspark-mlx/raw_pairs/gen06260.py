from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP"]) & ~F.col("city").isin(["New York", "London"]))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
)
