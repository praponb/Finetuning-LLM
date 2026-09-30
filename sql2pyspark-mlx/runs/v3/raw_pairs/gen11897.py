from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
)

result = (
    df_departments.alias("departments")
    .join(employees_by_department.alias("t"), F.col("departments.id") == F.col("t.dept_id"), "inner")
    .select(
        F.col("departments.id"),
        F.col("departments.location"),
        F.col("departments.budget"),
        F.col("t.unique_job_titles"),
    )
)
