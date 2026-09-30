from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3501000, "high").when(F.col("budget") >= 927000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy(F.col("budget_bucket").desc())
    .limit(1)
)
