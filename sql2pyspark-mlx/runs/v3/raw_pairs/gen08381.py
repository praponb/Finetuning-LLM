from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.is_remote"))
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy("manager_email")
)
