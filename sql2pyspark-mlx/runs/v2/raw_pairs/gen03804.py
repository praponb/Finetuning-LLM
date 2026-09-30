from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.full_name").like("%smit%") & (F.col("e.job_title") == "Manager"))
    .select(F.col("d.dept_name"), F.col("d.budget"), F.col("e.hire_date"))
)
