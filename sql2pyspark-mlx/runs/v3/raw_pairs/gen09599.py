from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter((F.col("mgr.job_title") == "Designer") & F.col("mgr.manager_id").isin([16, 21, 31]))
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
