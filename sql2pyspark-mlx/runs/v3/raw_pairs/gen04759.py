from pyspark.sql import functions as F

base = df_departments.filter(~F.col("location").isin(["Berlin", "Singapore", "London"]))

budget_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3496000, "high").when(F.col("budget") >= 1446000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("*").alias("total_count"),
    )
)

result = budget_bucket_summary.select("budget_bucket", "total_budget", "total_count")
