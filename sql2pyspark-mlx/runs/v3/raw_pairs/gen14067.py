from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.max_salary") >= 61000)
    .select(F.col("d.budget"), F.col("d.dept_name"), F.col("ds.max_salary"))
)
