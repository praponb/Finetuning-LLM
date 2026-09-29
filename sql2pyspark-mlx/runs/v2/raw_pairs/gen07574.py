from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.upper(F.col("e.email")).like("%VY@%"))
    .select(
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.job_title").alias("manager_job_title"),
    )
)
