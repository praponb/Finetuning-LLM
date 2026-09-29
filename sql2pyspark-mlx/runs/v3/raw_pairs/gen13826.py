from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.coalesce(F.col("emp.level"), F.lit("unassigned")).alias("level_filled"),
    )
)
