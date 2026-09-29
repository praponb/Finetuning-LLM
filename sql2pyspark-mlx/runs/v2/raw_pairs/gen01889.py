from pyspark.sql import functions as F

department_summary = (
    df_employees
    .filter(~F.col("full_name").like("%Chaiyapor%"))
    .groupBy("dept_id")
    .agg(
        F.sum(F.when(F.col("level") == "L2", 1).otherwise(0)).alias("l2_count"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
    )
)

result = (
    df_departments.alias("d")
    .join(department_summary.alias("ds"), F.col("d.id") == F.col("ds.dept_id"), "left")
    .select(
        F.col("d.id"),
        F.col("d.dept_name"),
        F.col("ds.l2_count"),
        F.col("ds.job_title_list"),
    )
)
