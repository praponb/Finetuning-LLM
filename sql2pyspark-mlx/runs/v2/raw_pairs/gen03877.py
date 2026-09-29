from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.concat(F.col("mgr.job_title"), F.lit(", "), F.col("mgr.level")).alias("job_title_level"),
    )
)
