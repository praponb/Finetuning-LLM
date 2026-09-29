from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.min("hire_date").alias("min_hire_date"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("ds.min_hire_date"),
        F.col("ds.job_title_list"),
    )
    .orderBy("dept_name")
)
