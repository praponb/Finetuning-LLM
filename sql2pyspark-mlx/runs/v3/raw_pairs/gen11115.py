from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.lower(F.col("e.full_name")).like("%mma%") | F.col("m.email").like("%@startup.io"))
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.datediff(F.current_date(), F.col("e.hire_date")).alias("days_since_hire_date"),
    )
)
