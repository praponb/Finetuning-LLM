from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter((F.col("emp.salary") < 78000) | (F.col("emp.salary") < 117000))
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
