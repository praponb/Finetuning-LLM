from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("dept_name") != "Finance")

location_summary = (
    recent_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4448000, "high").when(F.col("budget") >= 1263000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    location_summary
    .filter(F.col("max_budget") > 2816000)
    .select("location", "budget_bucket", "stddev_budget")
    .orderBy("stddev_budget", "location", "budget_bucket")
    .limit(1)
)
