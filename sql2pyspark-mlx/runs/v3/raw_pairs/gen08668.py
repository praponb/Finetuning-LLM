from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.salary") > 129000)
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.is_remote").alias("employee_is_remote"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
)
