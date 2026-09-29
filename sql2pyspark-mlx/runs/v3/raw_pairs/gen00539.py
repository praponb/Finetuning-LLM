from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.manager_id").isin([3, 15, 20]) | F.col("e.manager_id").isin([30, 35]))
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.trim(F.col("e.job_title")).alias("clean_job_title"),
    )
)
