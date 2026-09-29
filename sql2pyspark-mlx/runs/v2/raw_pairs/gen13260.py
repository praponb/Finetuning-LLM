from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(~F.col("e.is_remote") & F.col("e.hire_date").between("2024-01-01", "2024-08-01"))
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
    )
)
