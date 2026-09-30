from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.level") == "L4")
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("id").desc())
)
