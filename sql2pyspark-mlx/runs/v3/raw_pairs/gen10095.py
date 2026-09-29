from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
)
