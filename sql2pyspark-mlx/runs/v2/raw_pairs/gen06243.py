from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.location"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .orderBy(F.col("location").desc())
)
