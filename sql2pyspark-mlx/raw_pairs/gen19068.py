from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("level").isin(["L5", "L3"]))
    .groupBy("dept_id")
    .agg(F.round(F.stddev("salary"), 2).alias("stddev_salary"))
)

result = (
    df_departments.alias("departments")
    .join(department_stats.alias("department_stats"), F.col("departments.id") == F.col("department_stats.dept_id"), "left")
    .filter(F.col("department_stats.stddev_salary") > 38)
    .select(
        F.col("departments.id"),
        F.col("departments.dept_name"),
        F.col("department_stats.stddev_salary"),
    )
    .orderBy("id")
)
