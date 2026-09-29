from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.manager_id").isin([8, 11]))
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(F.count(F.col("e.id")).alias("employee_count"))
    .orderBy("dept_name", "id")
    .limit(50)
)
