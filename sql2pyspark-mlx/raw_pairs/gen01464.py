from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.full_name").like("Oli%"))
    .select(
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.email").alias("manager_email"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.col("mgr.level").alias("manager_level"),
    )
    .orderBy("manager_full_name", "employee_hire_date")
)
