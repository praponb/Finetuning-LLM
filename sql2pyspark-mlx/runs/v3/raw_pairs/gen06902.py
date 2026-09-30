from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "inner")
    .groupBy(F.col("departments.id"), F.col("departments.location"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.max(F.col("employees.salary")).alias("max_salary"),
    )
    .filter(F.col("employee_count") >= 5)
)
