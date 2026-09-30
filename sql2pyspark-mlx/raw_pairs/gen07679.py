from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3377000, "high").when(F.col("budget") >= 921000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum(F.when(F.col("dept_name") == "HR", F.col("budget")).otherwise(0)).alias("hr_budget"),
    )
    .filter(F.col("avg_budget") < 2108000)
    .orderBy(F.col("hr_budget").desc(), "budget_bucket")
    .limit(50)
)
