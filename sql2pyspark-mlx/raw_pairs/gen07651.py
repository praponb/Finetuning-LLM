from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("location") == "Tokyo")

summary = (
    valid_departments
    .groupBy(
        F.when(F.col("budget") >= 4301000, "high").when(F.col("budget") >= 1912000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = (
    summary
    .filter(F.col("total_count") < 5)
    .select("budget_bucket", "total_count")
)
