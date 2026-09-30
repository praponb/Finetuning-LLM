from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
)
