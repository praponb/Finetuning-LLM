from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .groupBy(F.col("d.id"), F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
        F.sort_array(F.collect_set(F.col("e.level"))).alias("level_list"),
    )
    .orderBy(F.col("id").desc())
)
