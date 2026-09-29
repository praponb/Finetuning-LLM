from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "left")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
    )
    .filter(F.col("total_count") >= 3)
    .orderBy(F.col("dept_name").desc(), "total_count")
)
