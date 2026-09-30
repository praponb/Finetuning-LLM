from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3301000, "high").when(F.col("budget") >= 1548000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("stddev_budget") >= 22)
    .select("budget_bucket", "dept_name", "stddev_budget", "budget_count")
)
