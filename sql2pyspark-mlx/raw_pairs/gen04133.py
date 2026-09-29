from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("manager_id") == 20)
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_job_titles"),
        F.min("salary").alias("min_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.unique_job_titles") >= 8)
    .select(F.col("d.budget"), F.col("ds.unique_job_titles"), F.col("ds.min_salary"))
)
