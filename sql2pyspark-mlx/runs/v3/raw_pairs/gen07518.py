from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4082000, "high").when(F.col("budget") >= 1603000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("avg_budget") >= 58)
)
