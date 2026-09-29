from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.salary") > 106000)
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("id", "employee_hire_date")
    .limit(50)
)
