from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter((F.col("location") != "New York") & (F.col("budget") >= 2213000))
)

agg = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4370000, "high").when(F.col("budget") >= 1724000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)

result = (
    agg
    .select("budget_bucket", "avg_budget", "budget_range")
    .orderBy("avg_budget")
)
