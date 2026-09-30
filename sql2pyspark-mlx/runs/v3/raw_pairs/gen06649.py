from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.manager_id") == 21) & F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.email").alias("manager_email"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.concat(F.col("m.job_title"), F.lit(" - "), F.col("m.email")).alias("job_title_email"),
    )
)
