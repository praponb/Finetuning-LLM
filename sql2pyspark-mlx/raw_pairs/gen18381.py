from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.lower(F.col("ch.full_name")).like("%any%")), "left_anti")
)

result = (
    kept
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Legal", F.col("budget")).otherwise(0)).alias("legal_budget"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("legal_budget") >= 0)
    .orderBy(F.col("avg_budget").desc_nulls_last())
)
