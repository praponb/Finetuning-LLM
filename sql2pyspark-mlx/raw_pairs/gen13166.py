from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.sort_array(F.collect_set("dept_name")).alias("dept_name_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("location") == "Tokyo", F.col("budget"))).alias("tokyo_max_budget"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Engineering", 1)), F.lit(0)), 2).alias("budget_per_engineering"),
    )
)
