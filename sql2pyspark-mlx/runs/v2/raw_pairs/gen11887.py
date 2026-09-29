from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.hire_date") <= "2025-02-04")
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("employee_hire_date")
)
