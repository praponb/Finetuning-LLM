from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.manager_id").isin([13, 21, 29, 34]))
    .select(
        F.col("emp.email").alias("employee_email"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
)
