from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.salary").alias("manager_salary"),
    )
)
