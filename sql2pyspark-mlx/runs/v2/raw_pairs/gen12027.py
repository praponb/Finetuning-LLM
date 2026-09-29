from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy("employee_full_name")
)
