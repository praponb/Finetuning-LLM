from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(~F.col("emp.full_name").like("P%"))
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
)
