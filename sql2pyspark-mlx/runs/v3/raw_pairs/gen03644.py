from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.coalesce(F.col("e.job_title"), F.lit("Unknown")).alias("job_title_filled"),
    )
)
