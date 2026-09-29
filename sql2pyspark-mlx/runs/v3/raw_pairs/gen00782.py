from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("departments.id") == F.col("employees.dept_id"), "left")
    .groupBy(F.col("departments.dept_name"))
    .agg(F.count(F.col("employees.id")).alias("employee_count"))
    .orderBy("dept_name")
)
