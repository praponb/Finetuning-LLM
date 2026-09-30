from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.sum("salary").alias("total_salary"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("employees_by_department"), F.col("departments.id") == F.col("employees_by_department.dept_id"), "left")
    .filter(F.col("employees_by_department.total_salary") > 697000)
    .select(
        F.col("departments.dept_name"),
        F.col("employees_by_department.total_salary"),
    )
    .orderBy(F.col("dept_name").desc(), "total_salary")
)
