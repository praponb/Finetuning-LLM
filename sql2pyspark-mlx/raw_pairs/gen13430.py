from pyspark.sql import functions as F

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (F.col("dept_name") == "Marketing"))
    .select("*", F.substring("dept_name", 1, 2).alias("dept_name_prefix"))
)

result = (
    valid
    .rollup("location")
    .agg(
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "HR", 1)), F.lit(0)), 2).alias("budget_per_hr"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
    )
    .orderBy(F.col("bangkok_count").desc_nulls_first())
)
