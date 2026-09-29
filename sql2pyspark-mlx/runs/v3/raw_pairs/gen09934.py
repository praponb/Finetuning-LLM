from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3607000, "high").when(F.col("budget") >= 1769000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("total_budget") >= 14155000)
)
