from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.round(F.col("e.salary"), 2).alias("salary_rounded"),
    )
)
