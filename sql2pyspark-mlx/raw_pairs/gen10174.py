from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.dept_id").isin([9, 15, 23, 26]))
    .select(F.col("e.full_name"), F.col("d.dept_name"))
)
