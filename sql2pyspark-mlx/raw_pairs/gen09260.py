from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("budget") > 2044000)

budget_bucket_summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3156000, "high").when(F.col("budget") >= 1725000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("dept_name").isin(["HR", "Marketing"]), "core").otherwise("other").alias("dept_name_group"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = (
    budget_bucket_summary
    .select("budget_bucket", "dept_name_group", "avg_budget", "budget_range")
)
