from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.manager_id") == 34)
    .select(
        F.col("emp.id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
)
