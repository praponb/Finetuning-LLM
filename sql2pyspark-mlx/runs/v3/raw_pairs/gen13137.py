from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.countDistinct(F.col("e.salary")).alias("unique_job_titles"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
    )
    .orderBy(F.col("id").desc())
)
