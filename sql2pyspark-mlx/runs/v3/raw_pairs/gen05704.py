from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.length(F.col("emp.email")) > 13)
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
    )
)
