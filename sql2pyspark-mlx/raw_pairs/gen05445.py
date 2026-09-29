from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("d.id") == F.col("e.dept_id"), "left")
    .filter(F.col("e.level") == "L4")
    .groupBy(F.col("d.dept_name"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.job_title")))).alias("job_title_values"),
        F.count(F.when(F.col("e.level") == "L2", 1)).alias("l2_count"),
    )
)
