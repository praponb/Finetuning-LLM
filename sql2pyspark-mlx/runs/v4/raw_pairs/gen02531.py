from pyspark.sql import functions as F

not_flagged = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.job_title") != "Manager"), "left_semi")
)

result = (
    not_flagged
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
    )
    .orderBy(F.col("min_budget").desc())
)
