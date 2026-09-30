from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.full_name").like("%Emm%") & F.col("emp.full_name").isNotNull())
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.salary").alias("manager_salary"),
    )
)
