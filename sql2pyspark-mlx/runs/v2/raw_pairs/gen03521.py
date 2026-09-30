from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.hire_date") > "2025-04-01")
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.job_title").alias("manager_job_title"),
        F.initcap(F.trim(F.col("e.level"))).alias("level_title"),
    )
)
