from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(~F.col("m.level").isin(["L5", "L1", "L3"]))
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.level").alias("manager_level"),
        F.col("m.full_name").alias("manager_full_name"),
        F.ceil(F.col("e.salary")).alias("salary_whole"),
    )
)
