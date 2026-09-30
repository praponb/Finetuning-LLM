from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.upper(F.col("m.full_name")).like("%ARO%"))
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
