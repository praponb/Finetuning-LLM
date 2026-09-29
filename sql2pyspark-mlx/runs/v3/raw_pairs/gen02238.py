from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.year(F.col("m.hire_date")) == 2025)
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.email").alias("manager_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
)
