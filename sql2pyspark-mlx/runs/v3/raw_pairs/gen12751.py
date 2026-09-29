from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~((F.col("e.level") == "L5") & (F.col("e.level") == "L5")))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.level").alias("manager_level"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.email").alias("manager_email"),
        F.datediff(F.current_date(), F.col("m.hire_date")).alias("days_since_hire_date"),
    )
)
