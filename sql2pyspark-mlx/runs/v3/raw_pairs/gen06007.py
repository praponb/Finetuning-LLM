from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.level").alias("manager_level"),
        F.col("m.email").alias("manager_email"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("employee_email", F.col("employee_full_name").desc())
)
