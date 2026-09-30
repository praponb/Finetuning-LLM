from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("job_title").isin(["Engineer", "Intern"]))
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "inner")
    .select(F.col("d.location"), F.col("d.dept_name"), F.col("t.level_list"))
)
