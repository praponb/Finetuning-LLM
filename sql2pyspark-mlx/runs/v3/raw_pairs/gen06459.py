from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.level").isin(["L5", "L2"]))
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.email").alias("manager_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
    )
    .orderBy("manager_email", "employee_job_title", "id")
    .limit(1)
)
