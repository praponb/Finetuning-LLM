from pyspark.sql import functions as F

department_stats = (
    df_employees
    .filter(F.col("email").like("%@mail.co.th"))
    .groupBy("dept_id")
    .agg(F.sort_array(F.collect_set("level")).alias("level_list"))
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(F.col("d.dept_name"), F.col("d.location"), F.col("ds.level_list"))
    .orderBy(F.col("location").desc())
)
