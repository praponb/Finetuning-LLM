from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.count(F.when(F.col("level") == "L2", 1)).alias("l2_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("dt.l2_count"))
    .orderBy("l2_count")
)
