from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.filter(F.col("location").isin(["Bangkok", "New York"])).alias("d"), F.col("e.dept_id") == F.col("d.id"), "left_semi")
    .filter(F.col("e.dept_id").isin([11, 20, 33]))
    .select(F.col("e.id"), F.col("e.email"))
)
