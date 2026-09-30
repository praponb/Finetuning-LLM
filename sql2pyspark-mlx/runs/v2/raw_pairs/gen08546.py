from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("m.job_title").alias("manager_job_title"),
    )
)
