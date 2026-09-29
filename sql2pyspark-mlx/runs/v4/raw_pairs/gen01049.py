from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["New York", "Tokyo", "Chiang Mai"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.level").isin(["L1", "L5", "L4"]))
    .select(F.col("e.id"), F.col("e.hire_date"), F.col("e.email"))
)
