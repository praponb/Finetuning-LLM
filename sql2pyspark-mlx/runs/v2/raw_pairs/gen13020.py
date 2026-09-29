from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.min(F.col("e.salary")).alias("min_salary"),
        F.countDistinct(F.col("e.salary")).alias("unique_job_titles"),
    )
    .orderBy("employee_count")
)
