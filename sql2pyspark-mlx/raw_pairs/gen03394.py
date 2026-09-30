from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("employees_by_department.latest_hire_date"),
    )
    .orderBy(F.col("latest_hire_date").desc(), "id")
    .limit(3)
)
