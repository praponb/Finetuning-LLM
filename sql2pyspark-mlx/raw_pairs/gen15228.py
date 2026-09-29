from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary") <= 102000), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("budget") >= 3592000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.size(F.collect_set("location")).alias("num_locations"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles_2"),
        F.size(F.collect_set("location")).alias("num_locations_2"),
    )
    .filter(F.col("budget_stddev") >= 0)
    .orderBy(F.col("budget_stddev").desc())
)
