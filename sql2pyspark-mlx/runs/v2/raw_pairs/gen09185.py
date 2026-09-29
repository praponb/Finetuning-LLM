from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "JP", 1).otherwise(0))) / F.count("*")), 1).alias("pct_jp"),
    )
    .orderBy("pct_jp")
)
