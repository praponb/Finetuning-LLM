from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.location"), F.col("t.l3_count"))
)
