from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.hire_date") > "2023-09-01")
    .select(
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.level").alias("manager_level"),
        F.col("m.salary").alias("manager_salary"),
    )
)
