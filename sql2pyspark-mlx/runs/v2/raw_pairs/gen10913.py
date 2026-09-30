from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .groupBy(F.col("departments.id"), F.col("departments.dept_name"))
    .agg(
        F.count(F.col("employees.id")).alias("employee_count"),
        F.round(F.avg(F.col("employees.salary")), 2).alias("avg_salary"),
    )
    .filter(F.col("avg_salary") > 66)
)
