from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.year("hire_date") >= 2024)
    .groupBy("dept_id")
    .agg(
        F.avg("salary").alias("avg_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(
        F.col("d.budget"),
        F.col("d.location"),
        F.col("dt.avg_salary"),
        F.col("dt.job_title_values"),
    )
    .orderBy("job_title_values", F.col("budget").desc())
)
