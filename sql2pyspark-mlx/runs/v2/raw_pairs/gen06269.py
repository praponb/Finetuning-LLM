from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.level").alias("employee_level"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
)
