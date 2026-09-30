from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.salary").between(62000, 130000))
    .select(
        F.col("emp.id"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.level").alias("manager_level"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
    )
)
