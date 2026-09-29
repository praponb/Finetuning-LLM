from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.cnt") >= 6)
    .select(F.col("d.location"), F.col("ds.cnt"))
    .orderBy(F.col("cnt").desc(), F.col("location").desc())
)
