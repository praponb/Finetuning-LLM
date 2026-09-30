from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "inner")
    .filter(F.col("d.dept_name") == "HR")
    .select(F.col("e.dept_id"), F.col("e.salary"), F.col("d.dept_name"))
)
