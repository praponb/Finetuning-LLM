from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.level").alias("manager_level"),
    )
)
