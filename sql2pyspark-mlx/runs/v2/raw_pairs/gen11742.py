from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.initcap(F.trim(F.col("emp.email"))).alias("email_title"),
    )
)
