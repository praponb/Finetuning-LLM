from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.length(F.col("e.full_name")) >= 14)
    .groupBy(F.col("d.location"), F.col("d.dept_name"))
    .agg(F.round(F.avg(F.col("e.salary")), 2).alias("avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
