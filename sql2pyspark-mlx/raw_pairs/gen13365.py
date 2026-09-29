from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.job_title").alias("manager_job_title"),
    )
)
