from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.analyst_count") >= 24)
    .select(F.col("d.dept_name"), F.col("ds.analyst_count"))
)
