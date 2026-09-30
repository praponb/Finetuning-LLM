from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("manager_id") == 36)
    .groupBy("dept_id")
    .agg(F.countDistinct("salary").alias("unique_job_titles"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(F.col("d.location"), F.col("d.dept_name"), F.col("D.unique_job_titles"))
    .orderBy("dept_name")
)
