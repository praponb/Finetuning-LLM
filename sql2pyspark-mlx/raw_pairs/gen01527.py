from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("manager_id").isin([1, 9])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_semi")
    .select(F.col("d.id"), F.col("d.location"), F.col("d.dept_name"))
)
