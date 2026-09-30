from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("d.id") == F.col("e.dept_id"), "left")
    .select(F.col("e.full_name"), F.col("d.budget"), F.col("d.dept_name"))
)
