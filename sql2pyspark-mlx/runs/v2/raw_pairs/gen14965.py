from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.dept_name"))
    .agg(F.min(F.col("e.hire_date")).alias("first_hire_date"))
    .orderBy(F.col("dept_name").desc())
)
