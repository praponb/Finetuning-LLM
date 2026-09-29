from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter((F.col("e.manager_id") == 27) & (F.col("d.budget") >= 1597000))
    .select(F.col("d.location"), F.col("d.budget"), F.col("e.manager_id"))
)
