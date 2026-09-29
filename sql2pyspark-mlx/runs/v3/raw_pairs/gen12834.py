from pyspark.sql import functions as F

department_totals = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum(F.when(F.col("job_title") == "Analyst", 1).otherwise(0)).alias("analyst_count"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_totals.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .filter(F.col("t.analyst_count") > 42)
    .select(
        F.col("d.dept_name"),
        F.col("d.budget"),
        F.col("t.level_list"),
        F.col("t.analyst_count"),
    )
    .orderBy("analyst_count")
)
