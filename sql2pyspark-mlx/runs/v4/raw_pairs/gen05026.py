from pyspark.sql import functions as F

department_summary = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .filter(F.col("t.stddev_salary") > 50)
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("t.stddev_salary"),
        F.col("t.job_title_values"),
    )
    .orderBy("id", "job_title_values")
    .limit(10)
)
