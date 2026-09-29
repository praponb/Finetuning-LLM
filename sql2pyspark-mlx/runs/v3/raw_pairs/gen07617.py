from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.concat(F.col("mgr.level"), F.lit(" - "), F.col("mgr.email")).alias("level_email"),
    )
)
