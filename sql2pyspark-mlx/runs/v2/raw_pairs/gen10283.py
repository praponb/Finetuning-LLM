from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("hire_date") < "2023-12-01").alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .filter(F.col("d.dept_name") == "Sales")
    .select(F.col("d.id"), F.col("d.dept_name"))
)
