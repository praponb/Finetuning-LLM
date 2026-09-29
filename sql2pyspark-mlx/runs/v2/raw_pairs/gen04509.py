from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter((F.col("e.dept_id") == 13) | (F.col("e.job_title") == "Analyst"))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
