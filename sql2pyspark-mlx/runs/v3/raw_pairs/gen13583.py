from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.job_title").isin(["Analyst", "Designer", "Intern"]) & (F.col("e.hire_date") > "2025-02-01"))
    .select(
        F.col("e.id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.level").alias("manager_level"),
    )
)
