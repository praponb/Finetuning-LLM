from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.length(F.col("emp.email")) < 6)
    .select(
        F.col("emp.id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.upper(F.trim(F.col("emp.full_name"))).alias("full_name_normalized"),
    )
)
