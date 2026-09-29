from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .groupBy(
        F.when(F.col("d.budget") >= 3477000, "high").when(F.col("d.budget") >= 1307000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set(F.col("e.job_title"))).alias("job_title_list"),
    )
    .orderBy("budget_bucket")
)
