from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Bangkok", "London"]) & F.col("dept_name").isin(["Sales", "Marketing"]) & F.col("dept_name").isin(["Legal", "HR", "Finance"]))
    .groupBy(
        F.when(F.col("budget") >= 3785000, "high").when(F.col("budget") >= 1935000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.countDistinct("budget").alias("unique_locations"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("unique_locations") < 5)
)
