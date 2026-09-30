from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("departments.budget").between(3055000, 3460000))
    .select(
        F.col("departments.budget"),
        F.col("departments.dept_name"),
        F.col("departments.location"),
        F.col("employees.salary"),
        F.col("employees.dept_id"),
        F.col("employees.hire_date"),
        F.when(F.col("departments.budget") >= 4000000, "High").when(F.col("departments.budget") >= 1518000, "Medium").otherwise("Low").alias("budget_tier"),
    )
)
