from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.manager_id").isNull())
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.job_title").alias("manager_job_title"),
    )
    .orderBy("manager_job_title")
)
