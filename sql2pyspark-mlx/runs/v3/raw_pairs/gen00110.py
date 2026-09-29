from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("dept_id").isin([18, 23, 40]))
    .groupBy("dept_id")
    .agg(F.avg("salary").alias("avg_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ds.avg_salary"))
)
