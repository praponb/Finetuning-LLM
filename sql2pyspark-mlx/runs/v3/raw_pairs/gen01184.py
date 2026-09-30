from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email"),
        F.col("e.level").alias("employee_level"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.email").alias("manager_email"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
)
