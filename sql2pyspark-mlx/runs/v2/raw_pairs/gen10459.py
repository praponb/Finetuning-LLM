from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("e.level") != "L4")
    .groupBy(F.col("d.dept_name"))
    .agg(F.max(F.col("e.salary")).alias("max_salary"))
    .orderBy(F.col("dept_name").desc())
    .limit(10)
)
