from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.hire_date").between("2023-07-25", "2025-01-01"))
    .select(
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
