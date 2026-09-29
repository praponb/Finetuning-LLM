from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("job_title").isNotNull())
    .groupBy("dept_id")
    .agg(F.count("*").alias("employee_count"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("t.employee_count"),
    )
    .orderBy("employee_count")
)
