from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.job_title").isin(["Engineer", "Director"]) & F.col("e.email").like("%@startup.io"))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy("employee_email", "employee_job_title")
)
