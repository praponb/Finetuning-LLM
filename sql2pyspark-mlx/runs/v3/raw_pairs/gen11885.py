from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Intern"]))
    .groupBy("dept_id")
    .agg(
        F.round(F.stddev("salary"), 2).alias("stddev_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .filter(F.col("ds.stddev_salary") > 72)
    .select(
        F.col("d.budget"),
        F.col("d.dept_name"),
        F.col("ds.stddev_salary"),
        F.col("ds.job_title_values"),
    )
    .orderBy(F.col("stddev_salary").desc())
)
