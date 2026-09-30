from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("salary") > 108000)
    .groupBy("dept_id")
    .agg(
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_count"),
        F.round(F.avg("salary"), 2).alias("avg_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.budget"),
        F.col("ds.analyst_count"),
        F.col("ds.avg_salary"),
    )
)
