from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.manager_id").isin([19, 30, 32]) & (F.col("e.hire_date") > "2024-10-23"))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.full_name").alias("manager_full_name"),
        F.initcap(F.trim(F.col("m.full_name"))).alias("full_name_title"),
    )
)
