from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.email").alias("manager_email"),
    )
)
