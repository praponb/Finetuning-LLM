from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.location"), F.col("d.dept_name"))
    .agg(F.countDistinct(F.col("e.salary")).alias("unique_managers"))
)
