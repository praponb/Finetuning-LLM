from pyspark.sql import functions as F

result = (
    df_departments.alias("departments")
    .join(df_employees.alias("employees"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("employees.salary") <= 112000)
    .groupBy(F.col("departments.id"), F.col("departments.location"))
    .agg(F.count(F.col("employees.id")).alias("employee_count"))
    .orderBy(F.col("id").desc())
)
