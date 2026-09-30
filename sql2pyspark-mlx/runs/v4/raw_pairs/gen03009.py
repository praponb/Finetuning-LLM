from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.manager_id") == 11)
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.email").alias("employee_email"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("employee_email").desc(), "id")
    .limit(10)
)
