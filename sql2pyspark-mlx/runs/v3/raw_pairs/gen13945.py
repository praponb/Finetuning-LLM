from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Marketing", "Engineering", "HR"]))
    .groupBy(
        F.when(F.col("budget") >= 3859000, "high").when(F.col("budget") >= 1721000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") >= 2669000)
    .select("budget_bucket", "avg_budget", "max_budget")
)
