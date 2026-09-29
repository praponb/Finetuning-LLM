from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(~F.col("is_remote"))
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "inner")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ds.job_title_list"),
    )
)
