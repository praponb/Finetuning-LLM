from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.level").isin(["L3", "L4"]))
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .filter(F.col("employee_count") >= 2)
    .orderBy("dept_name")
)
