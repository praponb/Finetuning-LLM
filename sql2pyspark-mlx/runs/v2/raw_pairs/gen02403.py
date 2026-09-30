from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3984000, "high").when(F.col("budget") >= 1706000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
        F.sum("budget").alias("total_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") < 3494000)
    .drop("avg_budget")
)
