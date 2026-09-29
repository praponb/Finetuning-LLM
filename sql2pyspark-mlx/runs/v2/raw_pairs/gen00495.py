from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.manager_id").isNotNull())
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.salary").alias("manager_salary"),
        F.concat_ws("-", F.col("e.level"), F.col("e.job_title")).alias("level_job_title_key"),
    )
    .orderBy("level_job_title_key")
)
