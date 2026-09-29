from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.job_title") != "Intern")
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.level").alias("manager_level"),
    )
)
