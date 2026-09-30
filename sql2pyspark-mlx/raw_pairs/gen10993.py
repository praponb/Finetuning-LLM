from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.manager_id") == 7)
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
    .orderBy("manager_job_title")
)
