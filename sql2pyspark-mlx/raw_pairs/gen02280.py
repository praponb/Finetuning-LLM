from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.max("hire_date").alias("latest_hire_date"),
        F.avg("salary").alias("avg_salary"),
    )
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("EmployeesByDepartment"), F.col("departments.id") == F.col("EmployeesByDepartment.dept_id"), "left")
    .filter(F.col("EmployeesByDepartment.avg_salary") >= 123000)
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("EmployeesByDepartment.latest_hire_date"),
        F.col("EmployeesByDepartment.avg_salary"),
    )
)
