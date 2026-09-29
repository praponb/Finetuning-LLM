from pyspark.sql import functions as F

employees_by_department = (
    df_employees
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
)

result = (
    df_departments.alias("d")
    .join(employees_by_department.alias("ebd"), F.col("d.id") == F.col("ebd.dept_id"), "inner")
    .filter(F.col("ebd.unique_job_titles") >= 7)
    .select(
        F.col("d.id"),
        F.col("d.location"),
        F.col("d.budget"),
        F.col("ebd.unique_job_titles"),
    )
)
