from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["HR", "Finance"]) & F.col("dept_name").isin(["Engineering", "Marketing"]) & (F.col("budget") < 3764000))
)

summary = (
    valid_departments
    .groupBy("dept_name")
    .agg(F.count("budget").alias("budget_count"))
)

result = summary.select("dept_name", "budget_count")
