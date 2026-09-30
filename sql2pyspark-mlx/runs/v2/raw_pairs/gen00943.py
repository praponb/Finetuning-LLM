from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4171000, "high").when(F.col("budget") >= 1073000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") < 8)
    .select("budget_bucket", "dept_name", "avg_budget", "tokyo_count")
)
