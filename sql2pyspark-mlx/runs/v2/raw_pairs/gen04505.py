from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.dept_name").isin(["Engineering", "Sales"]))
    .groupBy(F.col("d.id"), F.col("d.location"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .filter(F.col("employee_count") >= 5)
    .orderBy(F.col("id").desc(), "location")
)
