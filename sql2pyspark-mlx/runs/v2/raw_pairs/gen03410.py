from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Finance") & (F.col("budget") < 3097000))
    .groupBy(
        F.when(F.col("budget") >= 3254000, "high").when(F.col("budget") >= 1554000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .filter(F.col("singapore_budget") >= 60)
)
