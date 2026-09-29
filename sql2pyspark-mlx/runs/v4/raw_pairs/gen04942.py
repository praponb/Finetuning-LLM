from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4428000, "high").when(F.col("budget") >= 768000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
    .filter(F.col("berlin_count") < 25)
)
