from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4015000, "high").when(F.col("budget") >= 725000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.count(F.when(F.col("dept_name") == "Legal", 1)).alias("legal_count"),
    )
)
