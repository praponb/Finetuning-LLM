from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.lower(F.trim("full_name")) == "jack taylor")
    .groupBy("dept_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("dt.latest_hire_date"))
)
