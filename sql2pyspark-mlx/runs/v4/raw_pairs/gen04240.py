from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.job_title").isin(["Designer", "Director", "Analyst"]))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.email").alias("manager_email"),
        F.col("m.level").alias("manager_level"),
    )
)
