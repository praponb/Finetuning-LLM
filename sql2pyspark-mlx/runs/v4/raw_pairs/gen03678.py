from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.job_title") == "Designer")
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
