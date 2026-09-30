from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") > 85000)
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.avg_salary") > 64)
    .select(F.col("d.id"), F.col("d.location"), F.col("D.avg_salary"))
    .orderBy("id")
)
