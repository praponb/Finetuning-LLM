from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.l3_count") >= 7)
    .select(F.col("d.budget"), F.col("d.dept_name"), F.col("dt.l3_count"))
    .orderBy("l3_count")
)
