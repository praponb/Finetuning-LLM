from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L1", 1)).alias("l1_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("dt.l1_count"))
    .orderBy("dept_name", "l1_count")
)
