from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4232000, "high").when(F.col("budget") >= 1662000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("avg_budget") < 1739000)
)
