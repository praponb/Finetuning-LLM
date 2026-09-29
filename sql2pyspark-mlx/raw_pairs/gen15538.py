from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.full_name").like("Gra%"))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
