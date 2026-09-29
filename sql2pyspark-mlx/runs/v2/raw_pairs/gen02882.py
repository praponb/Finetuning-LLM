from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter((F.col("mgr.job_title") != "Designer") | F.col("emp.is_remote"))
    .select(
        F.col("emp.id"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.email").alias("manager_email"),
    )
    .orderBy(F.col("id").desc())
)
