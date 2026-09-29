from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.salary").between(110000, 127000) & (F.col("e.hire_date") >= "2025-08-01"))
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
