from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.level") == "L3")
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.salary").alias("manager_salary"),
    )
    .orderBy("employee_full_name")
)
