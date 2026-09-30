from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.sum(F.when(F.col("level") == "L5", 1).otherwise(0)).alias("l5_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .filter(F.col("ds.l5_count") > 37)
    .select(F.col("d.budget"), F.col("ds.stddev_salary"), F.col("ds.l5_count"))
    .orderBy("budget")
)
