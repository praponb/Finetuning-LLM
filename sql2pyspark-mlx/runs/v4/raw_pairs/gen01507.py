from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.dept_name"), F.col("d.location"))
    .agg(
        F.avg(F.col("e.salary")).alias("avg_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.job_title")))).alias("job_title_values"),
    )
    .orderBy("avg_salary")
)
