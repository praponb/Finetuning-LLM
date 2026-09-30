from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.salary").alias("employee_salary"),
        F.col("m.job_title").alias("manager_job_title"),
        F.substring(F.col("m.job_title"), 1, 4).alias("job_title_prefix"),
    )
)
