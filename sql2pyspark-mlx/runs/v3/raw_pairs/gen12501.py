from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.min("salary").alias("min_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.min_salary") >= 78000)
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ds.min_salary"))
)
