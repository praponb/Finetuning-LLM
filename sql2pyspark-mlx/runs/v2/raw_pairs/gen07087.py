from pyspark.sql import functions as F

result = (
    df_departments.alias("d")
    .join(df_employees.alias("e"), F.col("e.dept_id") == F.col("d.id"), "left")
    .filter(F.col("e.level").isin(["L3", "L5"]))
    .groupBy(F.col("d.location"))
    .agg(
        F.count(F.col("e.id")).alias("employee_count"),
        F.max(F.col("e.hire_date")).alias("latest_hire_date"),
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
    )
    .orderBy("employee_count")
)
