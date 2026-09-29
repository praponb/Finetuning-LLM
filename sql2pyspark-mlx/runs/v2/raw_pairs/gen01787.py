from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal", "HR"]))
    .groupBy(
        F.when(F.col("budget") >= 3475000, "high").when(F.col("budget") >= 2014000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0))) / F.count("*")), 1).alias("pct_engineering"),
    )
)
