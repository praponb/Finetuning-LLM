from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy(F.col("manager_full_name").desc())
)
