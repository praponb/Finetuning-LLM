from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.email").alias("manager_email"),
        F.substring(F.col("emp.level"), 1, 4).alias("level_prefix"),
    )
)
