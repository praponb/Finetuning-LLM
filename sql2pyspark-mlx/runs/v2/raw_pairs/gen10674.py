from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "outer")
    .groupBy(F.col("d.dept_name"), F.col("d.location"))
    .agg(F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"))
)
