from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("departments.id") == F.col("employees.dept_id"), "inner")
    .filter(F.col("employees.job_title").isin(["Manager", "Analyst", "Designer"]))
    .groupBy(F.col("departments.location"))
    .agg(
        F.round(F.avg(F.col("employees.salary")), 2).alias("avg_salary"),
        F.max(F.col("employees.hire_date")).alias("last_hire_date"),
    )
)
