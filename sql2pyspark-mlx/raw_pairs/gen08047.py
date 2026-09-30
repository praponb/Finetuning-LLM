from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.full_name").like("%Brown%"))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.level").alias("manager_level"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.email").alias("manager_email"),
    )
    .orderBy(F.col("manager_salary").desc(), F.col("manager_level").desc())
)
