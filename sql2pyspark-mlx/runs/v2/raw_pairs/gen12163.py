from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.level").alias("manager_level"),
        F.lower(F.col("m.email")).alias("email_lower"),
    )
)
