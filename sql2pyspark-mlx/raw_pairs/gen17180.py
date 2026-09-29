from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.location"), F.col("d.dept_name"), F.col("t.analyst_count"))
)
