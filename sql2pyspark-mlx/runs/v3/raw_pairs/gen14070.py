from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.email").like("%@yahoo.com"))
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy("employee_email")
)
