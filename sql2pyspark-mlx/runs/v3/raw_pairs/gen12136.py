from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L5", 1)).alias("l5_count"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("D"), F.col("d.id") == F.col("D.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("D.l5_count"))
    .orderBy("l5_count")
)
