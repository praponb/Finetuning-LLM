from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .filter(F.col("departments.location") == "New York")
    .groupBy(F.col("departments.id"), F.col("departments.location"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.max(F.col("employees.hire_date")).alias("latest_hire_date"),
        F.round(F.avg(F.col("employees.salary")), 2).alias("avg_salary"),
    )
    .filter(F.col("employee_count") > 1)
)
