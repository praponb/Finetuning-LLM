from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary") >= 119000)
    .groupBy("dept_id")
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.dept_name"), F.col("t.avg_salary"), F.col("t.stddev_salary"))
    .orderBy("dept_name")
)
