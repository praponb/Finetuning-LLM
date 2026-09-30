from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter((F.col("m.job_title") == "Director") | F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.job_title").alias("manager_job_title"),
    )
)
