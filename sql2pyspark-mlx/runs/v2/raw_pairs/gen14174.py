from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4367000, "high").when(F.col("budget") >= 1229000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        "location",
    )
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0))) / F.count("*")), 1).alias("pct_hr"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("avg_budget") >= 37)
    .orderBy("pct_hr", "finance_count", "budget_bucket", "dept_name", "location")
    .limit(5)
)
