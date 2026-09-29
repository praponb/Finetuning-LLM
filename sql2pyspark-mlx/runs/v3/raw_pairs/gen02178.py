from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.min_salary") > 103000)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("D.min_salary"))
)
