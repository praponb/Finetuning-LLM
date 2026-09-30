from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
        F.sum(F.when(F.col("e.job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)
