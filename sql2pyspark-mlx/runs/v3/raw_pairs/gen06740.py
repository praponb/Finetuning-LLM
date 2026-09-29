from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.row_count") >= 8)
    .select(F.col("d.dept_name"), F.col("t.row_count"))
    .orderBy(F.col("row_count").desc())
)
