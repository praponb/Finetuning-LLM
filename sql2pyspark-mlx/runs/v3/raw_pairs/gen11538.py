from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.lower(F.trim(F.col("mgr.full_name"))) == "olivia garcia")
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
