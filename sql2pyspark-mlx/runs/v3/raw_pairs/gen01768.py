from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("hire_date").between("2023-08-01", "2025-05-23")).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .filter(F.col("d.location").isNotNull())
    .select(F.col("d.id"), F.col("d.location"), F.col("d.budget"), F.col("d.dept_name"))
)
