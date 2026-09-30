from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3822000, "high").when(F.col("budget") >= 2056000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("stddev_budget") >= 42)
    .select("budget_bucket", "stddev_budget", "avg_budget")
    .orderBy(F.col("avg_budget").desc(), "budget_bucket")
    .limit(50)
)
