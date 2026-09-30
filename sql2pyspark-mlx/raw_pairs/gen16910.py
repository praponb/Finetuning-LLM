from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.level").alias("manager_level"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
        F.concat_ws("-", F.col("e.job_title"), F.col("e.full_name")).alias("job_title_full_name_key"),
    )
)
