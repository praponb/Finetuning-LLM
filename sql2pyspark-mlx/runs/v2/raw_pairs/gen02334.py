from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
    .orderBy(F.col("employee_job_title").desc())
)
