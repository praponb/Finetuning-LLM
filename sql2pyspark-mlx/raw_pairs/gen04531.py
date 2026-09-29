from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.id"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .filter(F.col("employee_count") > 4)
    .orderBy(F.col("employee_count").desc(), "id")
    .limit(20)
)
