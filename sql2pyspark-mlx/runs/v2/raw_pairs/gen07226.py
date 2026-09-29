from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.level") != "L1")
    .groupBy(F.col("d.location"))
    .agg(F.sum(F.col("e.salary")).alias("total_salary"))
    .filter(F.col("total_salary") >= 462000)
)
