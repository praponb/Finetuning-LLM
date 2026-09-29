from pyspark.sql import functions as F

department_stats = (
    df_employees
    .groupBy("dept_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_stats.alias("t"), F.col("d.id") == F.col("t.dept_id"), "left")
    .select(F.col("d.location"), F.col("t.unique_levels"), F.col("t.job_title_list"))
    .orderBy(F.col("location").desc())
)
