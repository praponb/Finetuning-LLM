from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "left")
    .select(
        F.col("departments.id"),
        F.col("departments.budget"),
        F.col("department_summary.unique_job_titles"),
    )
    .orderBy(F.col("budget").desc())
)
