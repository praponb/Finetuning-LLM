from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.level").isin(["L4", "L3"]))
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
)
