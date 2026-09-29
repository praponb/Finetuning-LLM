from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .filter(F.col("e.is_remote") & (F.col("d.budget") <= 1908000))
    .select(F.col("e.id"), F.col("e.dept_id"), F.col("d.location"))
)
