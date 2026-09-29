from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("e.level") != "L5") & F.col("e.full_name").isNull())
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.email").alias("manager_email"),
        F.col("m.full_name").alias("manager_full_name"),
    )
    .orderBy("manager_email")
)
