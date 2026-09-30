from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.hire_date") < "2024-03-06")
    .select(F.col("d.location"), F.col("e.is_remote"))
)
