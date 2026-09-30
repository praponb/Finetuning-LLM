from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.level").alias("employee_level"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("employee_level").desc())
)
