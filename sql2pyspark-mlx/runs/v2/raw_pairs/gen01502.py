from pyspark.sql import functions as F

result = (
    df_employees.alias("employees")
    .join(df_departments.alias("departments"), F.col("employees.dept_id") == F.col("departments.id"), "left")
    .filter(F.col("departments.location") == "New York")
    .groupBy(F.col("departments.dept_name"))
    .agg(
        F.count(F.when(F.col("employees.job_title") == "Designer", 1)).alias("designer_count"),
        F.round(F.avg(F.col("employees.salary")), 2).alias("avg_salary"),
    )
    .orderBy(F.col("dept_name").desc())
)
