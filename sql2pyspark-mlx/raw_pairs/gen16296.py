from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.full_name").like("FRA%"))
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
)
