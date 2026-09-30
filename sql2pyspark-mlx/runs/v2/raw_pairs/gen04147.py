from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_summary.alias("department_summary"), F.col("departments.id") == F.col("department_summary.dept_id"), "inner")
    .filter(F.col("department_summary.min_salary") > 87000)
    .select(F.col("departments.location"), F.col("department_summary.min_salary"))
)
