from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select(F.col("d.budget"), F.col("e.email"), F.col("e.level"))
    .orderBy(F.col("level").desc())
)
