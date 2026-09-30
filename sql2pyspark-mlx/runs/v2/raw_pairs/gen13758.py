from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.job_title")))).alias("job_title_values"),
        F.round(F.stddev(F.col("e.salary")), 2).alias("stddev_salary"),
    )
    .orderBy(F.col("dept_name").desc())
)
