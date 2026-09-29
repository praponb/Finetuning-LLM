from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("hire_date") >= "2025-03-04")
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.level_values"),
        F.col("ds.stddev_salary"),
    )
)
