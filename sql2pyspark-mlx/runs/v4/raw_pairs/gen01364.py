from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("employees.manager_id").isin([8, 9, 26, 28]))
    .groupBy(F.col("departments.id"), F.col("departments.location"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.min(F.col("employees.salary")).alias("min_salary"),
    )
    .orderBy(F.col("min_salary").desc())
)
