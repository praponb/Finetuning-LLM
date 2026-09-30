from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("salary").between(127000, 145000))
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(F.col("d.id"), F.col("d.dept_name"), F.col("ds.job_title_values"))
    .orderBy("job_title_values", "id")
    .limit(50)
)
