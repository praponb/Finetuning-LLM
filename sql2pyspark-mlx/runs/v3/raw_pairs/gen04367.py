from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "right")
    .groupBy(
        F.when(F.col("d.budget") >= 4173000, "high").when(F.col("d.budget") >= 1973000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max(F.col("e.salary")) - F.min(F.col("e.salary"))).alias("salary_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("e.job_title")))).alias("job_title_values"),
    )
    .orderBy("budget_bucket")
    .limit(5)
)
