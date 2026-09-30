from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.salary") < 133000)
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.email").alias("manager_email"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("manager_email").desc(), F.col("employee_manager_id").desc())
)
