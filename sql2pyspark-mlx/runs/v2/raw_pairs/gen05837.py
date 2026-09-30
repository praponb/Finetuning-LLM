from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.round(F.avg("salary"), 2).alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .filter(F.col("D.avg_salary") >= 67)
    .select(F.col("d.id"), F.col("d.budget"), F.col("D.avg_salary"))
    .orderBy(F.col("avg_salary").desc())
)
