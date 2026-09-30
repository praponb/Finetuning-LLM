from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(
        F.when(F.col("d.budget") >= 3866000, "high").when(F.col("d.budget") >= 1362000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.col("e.salary")).alias("total_salary"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
    )
    .orderBy("budget_bucket")
)
