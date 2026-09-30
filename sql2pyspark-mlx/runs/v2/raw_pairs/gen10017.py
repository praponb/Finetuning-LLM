from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.filter(F.col("manager_id").isin([3, 7, 10, 27])).alias("e"), F.col("d.id") == F.col("e.dept_id"), "left_anti")
    .select(F.col("d.id"), F.col("d.location"), F.col("d.dept_name"))
    .orderBy(F.col("id").desc())
)
