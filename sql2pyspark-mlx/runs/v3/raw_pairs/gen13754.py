from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
)
