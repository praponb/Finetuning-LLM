from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("dept_id").isin([3, 8, 9, 28]))
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.stddev_salary"),
        F.col("ds.level_values"),
    )
    .orderBy("level_values", F.col("budget").desc())
)
