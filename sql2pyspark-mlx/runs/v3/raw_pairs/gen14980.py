from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location") == "New York")

budget_bucket_summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4093000, "high").when(F.col("budget") >= 1725000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
)

result = budget_bucket_summary.select("budget_bucket", "stddev_budget")
