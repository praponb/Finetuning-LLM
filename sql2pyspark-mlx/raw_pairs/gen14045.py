from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.is_remote")), "left_anti")
)

result = (
    kept
    .groupBy("location")
    .agg(
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_count"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("hr_count").desc())
)
