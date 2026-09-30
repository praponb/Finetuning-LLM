from pyspark.sql import functions as F

valid_departments = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "HR", "Marketing"]) & F.col("dept_name").isin(["Sales", "HR", "Legal"]))
)

departments_stats = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 3755000, "high").when(F.col("budget") >= 602000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = departments_stats.filter(F.col("avg_budget") > 57)
