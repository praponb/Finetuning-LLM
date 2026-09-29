from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("hire_date") >= "2025-09-01").alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.budget").between(3042000, 3673000))
    .select(F.col("d.id"), F.col("d.budget"))
)
