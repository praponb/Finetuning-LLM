from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.stddev_salary") > 68)
    .select(F.col("d.id"), F.col("d.location"), F.col("ds.stddev_salary"))
    .orderBy("id", "location")
)
