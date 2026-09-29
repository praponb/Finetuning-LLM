from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4489000, "high").when(F.col("budget") >= 1880000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.sum("budget").alias("total_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .orderBy("avg_budget", F.col("budget_bucket").desc(), "location")
    .limit(1)
)
