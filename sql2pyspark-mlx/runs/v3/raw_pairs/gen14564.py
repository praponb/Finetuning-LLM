from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.dept_id").isin([12, 32, 39]))
    .select(
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy("employee_full_name")
)
