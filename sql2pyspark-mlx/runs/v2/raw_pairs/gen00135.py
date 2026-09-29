from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("dept_id").isin([2, 5]))
    .groupBy("dept_id")
    .agg(F.min("hire_date").alias("first_hire_date"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("employees_by_department.first_hire_date"),
    )
)
