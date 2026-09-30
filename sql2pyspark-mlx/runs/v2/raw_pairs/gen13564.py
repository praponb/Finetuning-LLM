from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Sales", "Legal", "HR"]))
    .groupBy(
        F.when(F.col("budget") >= 3783000, "high").when(F.col("budget") >= 1165000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("budget_range") < 49)
)
