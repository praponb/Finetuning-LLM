from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .filter(F.col("dt.total_count") >= 10)
    .select(F.col("d.id"), F.col("d.location"), F.col("dt.total_count"))
    .orderBy(F.col("total_count").desc(), "id")
    .limit(5)
)
