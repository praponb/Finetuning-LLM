from pyspark.sql import functions as F

department_totals = (
    df_employees
    .filter(F.col("job_title") == "Engineer")
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("dt"), F.col("d.id") == F.col("dt.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.budget"), F.col("dt.job_title_values"))
    .orderBy("job_title_values", F.col("id").desc())
)
