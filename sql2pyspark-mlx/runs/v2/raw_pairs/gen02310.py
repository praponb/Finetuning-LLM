from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.trim(F.col("e.level")).alias("clean_level"),
    )
    .orderBy(F.col("employee_hire_date").desc(), F.col("clean_level").desc())
)
