from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.email").like("%@company.co"))
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.level").alias("manager_level"),
    )
)
