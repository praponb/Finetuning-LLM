from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.dept_id") == 9)
    .select(
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
