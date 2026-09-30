from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.hire_date").between("2023-09-01", "2025-07-01"))
    .select(F.col("d.location"), F.col("e.full_name"), F.col("e.dept_id"))
)
