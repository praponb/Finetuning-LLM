from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("d.budget") > 2590000)
    .groupBy(F.col("d.id"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .orderBy("id", "employee_count")
)
