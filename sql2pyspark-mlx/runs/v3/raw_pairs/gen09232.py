from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.level") == "L5")
    .select(
        F.col("emp.id"),
        F.col("emp.email").alias("employee_email"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
    .orderBy(F.col("employee_full_name").desc())
)
